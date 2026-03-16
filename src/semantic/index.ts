/**
 * BM25 semantic index using SQLite FTS5.
 * Indexes table semantics for natural language search (intent routing).
 */

import Database from "better-sqlite3";
import crypto from "node:crypto";
import type { TableSemantic } from "../types.js";
import type { CatalogStore } from "../catalog/store.js";
import { getConfig } from "../config.js";
import { Embedder } from "./embedder.js";
import { updateCacheEntry } from "./hybridSearch.js";

export class SemanticIndex {
  private db: Database.Database;

  constructor(dbPath?: string) {
    const config = getConfig();
    this.db = new Database(dbPath ?? config.catalog.dbPath);
    this.initialize();
  }

  private initialize(): void {
    // Create FTS5 virtual table for semantic search
    this.db.exec(`
      CREATE VIRTUAL TABLE IF NOT EXISTS semantic_index USING fts5(
        source_id,
        table_name,
        searchable_text,
        tokenize='unicode61'
      );
    `);
  }

  /**
   * Build a searchable text entry from a table's semantic definition.
   * Concatenates: table name + description + tags + domain + column names + column semantics + enum values + metric names
   */
  buildSearchableText(semantic: TableSemantic): string {
    const parts: string[] = [
      semantic.table,
      semantic.description ?? "",
      ...(semantic.tags ?? []),
      semantic.businessDomain ?? "",
    ];

    for (const col of semantic.columns ?? []) {
      parts.push(col.name);
      if (col.semantic) parts.push(col.semantic);
      if (col.unit) parts.push(col.unit);
      if (col.enumValues) {
        parts.push(...Object.values(col.enumValues));
      }
    }

    for (const metric of semantic.metrics ?? []) {
      parts.push(metric.name);
    }

    // Filter empty strings and join
    return parts.filter((p) => p.trim()).join(" ");
  }

  /**
   * Index (or re-index) a single table's semantic.
   * Deletes existing entry first, then inserts new one.
   * If embedding is enabled and embedder is ready, also generates and saves the embedding.
   */
  indexTable(
    sourceId: string,
    tableName: string,
    semantic: TableSemantic,
    catalog?: CatalogStore,
    embedder?: Embedder,
  ): void {
    // Delete existing entry
    this.db
      .prepare("DELETE FROM semantic_index WHERE source_id = ? AND table_name = ?")
      .run(sourceId, tableName);

    // Insert new entry
    const searchableText = this.buildSearchableText(semantic);
    this.db
      .prepare("INSERT INTO semantic_index (source_id, table_name, searchable_text) VALUES (?, ?, ?)")
      .run(sourceId, tableName, searchableText);

    // Embedding: fire-and-forget if enabled and ready
    if (catalog && embedder?.isReady()) {
      const contentHash = crypto.createHash("sha256").update(searchableText).digest("hex").slice(0, 16);
      const existing = catalog.getEmbedding(sourceId, tableName);
      if (!existing || existing.contentHash !== contentHash || existing.modelId !== embedder.getModelId()) {
        embedder
          .embed(searchableText)
          .then((vector) => {
            catalog.saveEmbedding(sourceId, tableName, vector, embedder.getModelId(), contentHash);
            updateCacheEntry(sourceId, tableName, vector);
          })
          .catch(() => {
            // Non-fatal: embedding generation failed
          });
      }
    }
  }

  /**
   * Remove a table from the index.
   */
  removeTable(sourceId: string, tableName: string): void {
    this.db
      .prepare("DELETE FROM semantic_index WHERE source_id = ? AND table_name = ?")
      .run(sourceId, tableName);
  }

  /**
   * Search the index using BM25 ranking.
   * Returns source_id + table_name pairs ordered by relevance.
   */
  search(
    query: string,
    topK = 5,
  ): Array<{ sourceId: string; tableName: string; rank: number }> {
    const safeQuery = this.buildFts5Query(query);

    if (!safeQuery) {
      return [];
    }

    // 1. Try BM25 FTS5 search (fast, ranked)
    let results: Array<{ source_id: string; table_name: string; rank: number }> = [];
    try {
      results = this.db
        .prepare(
          `SELECT source_id, table_name, rank
           FROM semantic_index
           WHERE searchable_text MATCH ?
           ORDER BY rank
           LIMIT ?`,
        )
        .all(safeQuery, topK) as typeof results;
    } catch {
      // FTS5 query syntax error — fall through to LIKE
    }

    // 2. FTS5 returned nothing: fall back to LIKE substring search (handles Chinese compound words)
    if (results.length === 0) {
      results = this.fallbackSearch(query, topK).map((r) => ({
        source_id: r.sourceId,
        table_name: r.tableName,
        rank: r.rank,
      }));
    }

    return results.map((r) => ({
      sourceId: r.source_id,
      tableName: r.table_name,
      rank: r.rank,
    }));
  }

  /**
   * Get the total number of indexed entries.
   */
  count(): number {
    const row = this.db
      .prepare("SELECT COUNT(*) as cnt FROM semantic_index")
      .get() as { cnt: number };
    return row.cnt;
  }

  /**
   * Clear the entire index.
   */
  clear(): void {
    this.db.exec("DELETE FROM semantic_index");
  }

  /**
   * Ensure all indexed tables have embeddings.
   * Re-generates embeddings for tables where content_hash or model_id has changed.
   */
  async ensureEmbeddings(catalog: CatalogStore, embedder: Embedder): Promise<number> {
    if (!embedder.isReady()) return 0;

    // Get all indexed entries
    const rows = this.db
      .prepare("SELECT source_id, table_name, searchable_text FROM semantic_index")
      .all() as Array<{ source_id: string; table_name: string; searchable_text: string }>;

    let count = 0;
    for (const row of rows) {
      const contentHash = crypto.createHash("sha256").update(row.searchable_text).digest("hex").slice(0, 16);
      const existing = catalog.getEmbedding(row.source_id, row.table_name);

      if (!existing || existing.contentHash !== contentHash || existing.modelId !== embedder.getModelId()) {
        try {
          const vector = await embedder.embed(row.searchable_text);
          catalog.saveEmbedding(row.source_id, row.table_name, vector, embedder.getModelId(), contentHash);
          updateCacheEntry(row.source_id, row.table_name, vector);
          count++;
        } catch {
          // Non-fatal
        }
      }
    }

    if (count > 0) {
      process.stderr.write(`[lucid-skill] embedder: generated ${count} embedding(s)\n`);
    }
    return count;
  }

  /**
   * Build a safe FTS5 query from natural language input.
   * Splits into tokens and joins with OR for broad matching.
   */
  private buildFts5Query(query: string): string {
    // Tokenize: split on whitespace, remove punctuation, filter empty
    const tokens = query
      .replace(/[^\w\u4e00-\u9fff\s]/g, " ")
      .split(/\s+/)
      .filter((t) => t.length > 0);

    if (tokens.length === 0) return "";

    // Join with OR for broader matching
    return tokens.join(" OR ");
  }

  /**
   * Fallback search using LIKE when FTS5 query fails.
   */
  private fallbackSearch(
    query: string,
    topK: number,
  ): Array<{ sourceId: string; tableName: string; rank: number }> {
    // Split query into tokens and do multi-term LIKE matching (each token as a substring)
    const tokens = query
      .split(/\s+/)
      .filter((t) => t.length > 0);

    if (tokens.length === 0) return [];

    // Build WHERE clause: each token must appear somewhere in searchable_text
    // Score = number of matching tokens (more matches = higher rank)
    const seen = new Map<string, { source_id: string; table_name: string; matchCount: number }>();

    for (const token of tokens) {
      const pattern = `%${token}%`;
      const rows = this.db
        .prepare(
          `SELECT source_id, table_name
           FROM semantic_index
           WHERE searchable_text LIKE ?`,
        )
        .all(pattern) as Array<{ source_id: string; table_name: string }>;

      for (const row of rows) {
        const key = `${row.source_id}::${row.table_name}`;
        const existing = seen.get(key);
        if (existing) {
          existing.matchCount++;
        } else {
          seen.set(key, { source_id: row.source_id, table_name: row.table_name, matchCount: 1 });
        }
      }
    }

    // Sort by match count descending, take topK
    return Array.from(seen.values())
      .sort((a, b) => b.matchCount - a.matchCount)
      .slice(0, topK)
      .map((r) => ({
        sourceId: r.source_id,
        tableName: r.table_name,
        rank: -r.matchCount, // Negative so higher matchCount = lower (better) rank
      }));
  }
}
