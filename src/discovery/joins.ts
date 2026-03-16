/**
 * JOIN path discovery engine.
 * Three signals: FK constraints, column name matching, embedding similarity.
 * Plus indirect (1-hop) path inference.
 */

import crypto from "node:crypto";
import type { CatalogStore } from "../catalog/store.js";
import type { Embedder } from "../semantic/embedder.js";
import type { JoinPath } from "../types.js";

interface ColumnMeta {
  column_name: string;
  dtype: string;
}

interface TableColumns {
  sourceId: string;
  tableName: string;
  columns: ColumnMeta[];
}

/** Compute schema hash for cache invalidation: sorted table+column+type → SHA256 prefix. */
export function computeSchemaHash(catalog: CatalogStore, sourceId: string): string {
  const tables = catalog.getTables(sourceId);
  const parts: string[] = [];
  for (const t of tables) {
    const cols = catalog.getColumns(sourceId, t.table_name);
    for (const c of cols) {
      parts.push(`${t.table_name}.${c.column_name}:${c.dtype}`);
    }
  }
  parts.sort();
  return crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 16);
}

/** Check if two column types are compatible for JOIN. */
function typesCompatible(a: string, b: string): boolean {
  const norm = (t: string) => {
    const u = t.toUpperCase();
    if (u.includes("INT") || u.includes("BIGINT") || u.includes("SMALLINT") || u.includes("TINYINT")) return "INT";
    if (u.includes("VARCHAR") || u.includes("TEXT") || u.includes("CHAR") || u.includes("STRING")) return "STRING";
    if (u.includes("FLOAT") || u.includes("DOUBLE") || u.includes("DECIMAL") || u.includes("NUMERIC") || u.includes("REAL")) return "NUMERIC";
    if (u.includes("DATE") || u.includes("TIMESTAMP") || u.includes("TIME")) return "TEMPORAL";
    if (u.includes("BOOL")) return "BOOL";
    return u;
  };
  return norm(a) === norm(b);
}

/** Strip common suffixes for fuzzy matching. */
function stripIdSuffix(name: string): string {
  return name.replace(/[_-]?(id|key|code)$/i, "").toLowerCase();
}

/** Generate a stable path ID. */
function pathId(sourceId: string, tableA: string, tableB: string, condition: string): string {
  const raw = `${sourceId}|${tableA}|${tableB}|${condition}`;
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 16);
}

/**
 * Discover all JOIN paths for a given source.
 * Returns direct paths from three signals + indirect 1-hop paths.
 */
export async function discoverJoinPaths(
  catalog: CatalogStore,
  sourceId: string,
  embedder?: Embedder | null,
): Promise<JoinPath[]> {
  const tables = catalog.getTables(sourceId);
  const allTableCols: TableColumns[] = tables.map((t) => ({
    sourceId,
    tableName: t.table_name,
    columns: catalog.getColumns(sourceId, t.table_name).map((c) => ({
      column_name: c.column_name,
      dtype: c.dtype,
    })),
  }));

  // Collect candidates per table pair, keyed by "tableA|tableB"
  const pairCandidates = new Map<string, JoinPath[]>();

  const addCandidate = (jp: JoinPath) => {
    const key = [jp.tableA, jp.tableB].sort().join("|");
    if (!pairCandidates.has(key)) pairCandidates.set(key, []);
    pairCandidates.get(key)!.push(jp);
  };

  // ── Signal 1: FK constraints ──
  // FK info is stored in columns_meta as references in comment or via semantic layer.
  // For CSV sources, ForeignKey info comes from connector.getTableInfo().foreignKeys
  // which was persisted during schema collection. We check column comments for FK hints.
  // (In practice, CSV sources don't have FK constraints, but MySQL/PostgreSQL do.)
  // We look for columns whose comment contains FK reference patterns.
  for (const tc of allTableCols) {
    const cols = catalog.getColumns(sourceId, tc.tableName);
    for (const col of cols) {
      if (col.comment && col.comment.includes("->")) {
        // Pattern: "FK -> table.column"
        const match = col.comment.match(/FK\s*->\s*(\w+)\.(\w+)/i);
        if (match) {
          const refTable = match[1];
          const refCol = match[2];
          const condition = `${tc.tableName}.${col.column_name} = ${refTable}.${refCol}`;
          addCandidate({
            pathId: pathId(sourceId, tc.tableName, refTable, condition),
            sourceId,
            tableA: tc.tableName,
            tableB: refTable,
            joinType: "INNER",
            joinCondition: condition,
            confidence: 1.0,
            signalSource: "fk_constraint",
            sqlTemplate: `SELECT * FROM ${tc.tableName} JOIN ${refTable} ON ${condition}`,
            version: 1,
          });
        }
      }
    }
  }

  // ── Signal 2: Column name matching ──
  for (let i = 0; i < allTableCols.length; i++) {
    for (let j = i + 1; j < allTableCols.length; j++) {
      const tA = allTableCols[i];
      const tB = allTableCols[j];

      for (const colA of tA.columns) {
        for (const colB of tB.columns) {
          if (!typesCompatible(colA.dtype, colB.dtype)) continue;

          let confidence = 0;
          let signal = "";
          const colALower = colA.column_name.toLowerCase();
          const colBLower = colB.column_name.toLowerCase();

          // Rule 1: table_a.xxx_id = table_b.id (or vice versa)
          const tBSingular = tB.tableName.replace(/s$/, "").toLowerCase();
          const tASingular = tA.tableName.replace(/s$/, "").toLowerCase();

          if (
            (colALower === `${tBSingular}_id` && colBLower === "id") ||
            (colALower === `${tBSingular}_id` && colBLower === `${tBSingular}_id`)
          ) {
            confidence = 0.85;
            signal = "name_pattern:fk_id";
          } else if (
            (colBLower === `${tASingular}_id` && colALower === "id") ||
            (colBLower === `${tASingular}_id` && colALower === `${tASingular}_id`)
          ) {
            confidence = 0.85;
            signal = "name_pattern:fk_id";
          }
          // Rule 2: exact column name match
          else if (colALower === colBLower && colALower !== "id") {
            confidence = 0.65;
            signal = "name_pattern:exact_match";
          }
          // Rule 3: strip _id/_key suffix match
          else if (
            stripIdSuffix(colA.column_name) !== "" &&
            stripIdSuffix(colA.column_name) === stripIdSuffix(colB.column_name) &&
            (colALower.endsWith("_id") || colALower.endsWith("_key") ||
             colBLower.endsWith("_id") || colBLower.endsWith("_key"))
          ) {
            confidence = 0.5;
            signal = "name_pattern:stripped_suffix";
          }

          if (confidence > 0) {
            const condition = `${tA.tableName}.${colA.column_name} = ${tB.tableName}.${colB.column_name}`;
            addCandidate({
              pathId: pathId(sourceId, tA.tableName, tB.tableName, condition),
              sourceId,
              tableA: tA.tableName,
              tableB: tB.tableName,
              joinType: "INNER",
              joinCondition: condition,
              confidence,
              signalSource: signal,
              sqlTemplate: `SELECT * FROM ${tA.tableName} JOIN ${tB.tableName} ON ${condition}`,
              version: 1,
            });
          }
        }
      }
    }
  }

  // ── Signal 3: Embedding similarity (soft signal) ──
  if (embedder && embedder.isReady()) {
    const { Embedder: EmbedderClass } = await import("../semantic/embedder.js");
    for (let i = 0; i < allTableCols.length; i++) {
      for (let j = i + 1; j < allTableCols.length; j++) {
        const tA = allTableCols[i];
        const tB = allTableCols[j];

        const embA = catalog.getEmbedding(sourceId, tA.tableName);
        const embB = catalog.getEmbedding(sourceId, tB.tableName);

        if (embA && embB) {
          const cosine = EmbedderClass.cosineSimilarity(embA.vector, embB.vector);
          if (cosine > 0.7) {
            const embScore = cosine * 0.6;
            // Only add as candidate if no existing candidate for this pair has higher confidence
            const key = [tA.tableName, tB.tableName].sort().join("|");
            const existing = pairCandidates.get(key) ?? [];
            // Boost existing candidates with embedding score
            for (const c of existing) {
              if (c.signalSource.startsWith("name_pattern")) {
                const colScore = c.confidence;
                c.confidence = 0.6 * colScore + 0.4 * embScore;
                c.signalSource += "+embedding";
              }
            }
            // If no column-based candidate exists, add a pure embedding candidate
            if (existing.length === 0) {
              addCandidate({
                pathId: pathId(sourceId, tA.tableName, tB.tableName, `embedding:${cosine.toFixed(3)}`),
                sourceId,
                tableA: tA.tableName,
                tableB: tB.tableName,
                joinType: "INNER",
                joinCondition: `-- embedding similarity ${cosine.toFixed(3)}, manual verification needed`,
                confidence: embScore,
                signalSource: "embedding",
                sqlTemplate: `-- No auto-generated JOIN: tables are semantically related (cosine=${cosine.toFixed(3)}) but no matching columns found`,
                version: 1,
              });
            }
          }
        }
      }
    }
  }

  // ── Fusion: filter and keep top-3 per pair ──
  const directPaths: JoinPath[] = [];
  for (const [_key, candidates] of pairCandidates) {
    // FK constraints always win
    const fkPaths = candidates.filter((c) => c.signalSource === "fk_constraint");
    if (fkPaths.length > 0) {
      directPaths.push(...fkPaths);
      continue;
    }

    // Filter by minimum threshold
    const valid = candidates.filter((c) => c.confidence >= 0.4);
    // Sort by confidence desc, take top 3
    valid.sort((a, b) => b.confidence - a.confidence);
    directPaths.push(...valid.slice(0, 3));
  }

  // ── Indirect paths (1-hop) ──
  const indirectPaths: JoinPath[] = [];
  const directPairSet = new Set<string>();
  for (const p of directPaths) {
    directPairSet.add([p.tableA, p.tableB].sort().join("|"));
  }

  // Build adjacency map from direct paths
  const adjacency = new Map<string, Map<string, JoinPath>>();
  for (const p of directPaths) {
    if (!adjacency.has(p.tableA)) adjacency.set(p.tableA, new Map());
    if (!adjacency.has(p.tableB)) adjacency.set(p.tableB, new Map());
    // Keep highest-confidence path per direction
    const existAB = adjacency.get(p.tableA)!.get(p.tableB);
    if (!existAB || p.confidence > existAB.confidence) {
      adjacency.get(p.tableA)!.set(p.tableB, p);
    }
    const existBA = adjacency.get(p.tableB)!.get(p.tableA);
    if (!existBA || p.confidence > existBA.confidence) {
      adjacency.get(p.tableB)!.set(p.tableA, p);
    }
  }

  const tableNames = allTableCols.map((t) => t.tableName);
  for (let i = 0; i < tableNames.length; i++) {
    for (let j = i + 1; j < tableNames.length; j++) {
      const tA = tableNames[i];
      const tB = tableNames[j];
      const pairKey = [tA, tB].sort().join("|");

      // Skip if direct path exists
      if (directPairSet.has(pairKey)) continue;

      // Find intermediate tables
      const neighborsA = adjacency.get(tA);
      if (!neighborsA) continue;

      for (const [mid, pathAM] of neighborsA) {
        if (mid === tB) continue;
        const neighborsM = adjacency.get(mid);
        if (!neighborsM) continue;
        const pathMB = neighborsM.get(tB);
        if (!pathMB) continue;

        const conf = Math.min(pathAM.confidence, pathMB.confidence) * 0.8;
        if (conf < 0.4) continue;

        const condition = `${pathAM.joinCondition} AND ${pathMB.joinCondition}`;
        const sqlTemplate = `SELECT * FROM ${tA} JOIN ${mid} ON ${pathAM.joinCondition} JOIN ${tB} ON ${pathMB.joinCondition}`;

        indirectPaths.push({
          pathId: pathId(sourceId, tA, tB, `via:${mid}`),
          sourceId,
          tableA: tA,
          tableB: tB,
          joinType: "INNER",
          joinCondition: condition,
          confidence: conf,
          signalSource: `indirect:via_${mid}`,
          sqlTemplate,
          version: 1,
        });
      }
    }
  }

  return [...directPaths, ...indirectPaths];
}

/** Sentinel sourceId used for cross-source JOIN paths. */
export const CROSS_SOURCE_ID = "__cross__";

/**
 * Compute a schema hash covering ALL sources (for cross-source cache invalidation).
 */
export function computeCrossSourceSchemaHash(catalog: CatalogStore): string {
  const sources = catalog.getSources();
  const parts: string[] = [];
  for (const src of sources) {
    const tables = catalog.getTables(src.id);
    for (const t of tables) {
      const cols = catalog.getColumns(src.id, t.table_name);
      for (const c of cols) {
        parts.push(`${src.id}|${t.table_name}.${c.column_name}:${c.dtype}`);
      }
    }
  }
  parts.sort();
  return crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 16);
}

/**
 * Discover JOIN paths across ALL sources.
 * Gathers tables from every source and runs column-name matching (Signal 2) + embedding (Signal 3)
 * across source boundaries. FK constraints (Signal 1) are skipped for cross-source pairs since
 * FK metadata is only meaningful within a single database.
 */
export async function discoverCrossSourceJoinPaths(
  catalog: CatalogStore,
  embedder?: Embedder | null,
): Promise<JoinPath[]> {
  const sources = catalog.getSources();
  const allTableCols: TableColumns[] = [];

  for (const src of sources) {
    const tables = catalog.getTables(src.id);
    for (const t of tables) {
      allTableCols.push({
        sourceId: src.id,
        tableName: t.table_name,
        columns: catalog.getColumns(src.id, t.table_name).map((c) => ({
          column_name: c.column_name,
          dtype: c.dtype,
        })),
      });
    }
  }

  if (allTableCols.length < 2) return [];

  const pairCandidates = new Map<string, JoinPath[]>();

  const addCandidate = (jp: JoinPath) => {
    const key = [jp.tableA, jp.tableB].sort().join("|");
    if (!pairCandidates.has(key)) pairCandidates.set(key, []);
    pairCandidates.get(key)!.push(jp);
  };

  // ── Signal 1: FK constraints (only for same-source pairs) ──
  for (const tc of allTableCols) {
    const cols = catalog.getColumns(tc.sourceId, tc.tableName);
    for (const col of cols) {
      if (col.comment && col.comment.includes("->")) {
        const match = col.comment.match(/FK\s*->\s*(\w+)\.(\w+)/i);
        if (match) {
          const refTable = match[1];
          // Only add FK path if the referenced table is in the same source
          const refInSameSource = allTableCols.some(
            (t) => t.sourceId === tc.sourceId && t.tableName === refTable,
          );
          if (refInSameSource) {
            const refCol = match[2];
            const condition = `${tc.tableName}.${col.column_name} = ${refTable}.${refCol}`;
            addCandidate({
              pathId: pathId(CROSS_SOURCE_ID, tc.tableName, refTable, condition),
              sourceId: CROSS_SOURCE_ID,
              tableA: tc.tableName,
              tableB: refTable,
              joinType: "INNER",
              joinCondition: condition,
              confidence: 1.0,
              signalSource: "fk_constraint",
              sqlTemplate: `SELECT * FROM ${tc.tableName} JOIN ${refTable} ON ${condition}`,
              version: 1,
            });
          }
        }
      }
    }
  }

  // ── Signal 2: Column name matching (across all tables) ──
  for (let i = 0; i < allTableCols.length; i++) {
    for (let j = i + 1; j < allTableCols.length; j++) {
      const tA = allTableCols[i];
      const tB = allTableCols[j];

      // Skip same-table (shouldn't happen but guard)
      if (tA.tableName === tB.tableName) continue;

      for (const colA of tA.columns) {
        for (const colB of tB.columns) {
          if (!typesCompatible(colA.dtype, colB.dtype)) continue;

          let confidence = 0;
          let signal = "";
          const colALower = colA.column_name.toLowerCase();
          const colBLower = colB.column_name.toLowerCase();

          // Rule 1: table_a.xxx_id = table_b.id (or vice versa)
          const tBSingular = tB.tableName.replace(/s$/, "").toLowerCase();
          const tASingular = tA.tableName.replace(/s$/, "").toLowerCase();

          if (
            (colALower === `${tBSingular}_id` && colBLower === "id") ||
            (colALower === `${tBSingular}_id` && colBLower === `${tBSingular}_id`)
          ) {
            confidence = 0.85;
            signal = "name_pattern:fk_id";
          } else if (
            (colBLower === `${tASingular}_id` && colALower === "id") ||
            (colBLower === `${tASingular}_id` && colALower === `${tASingular}_id`)
          ) {
            confidence = 0.85;
            signal = "name_pattern:fk_id";
          }
          // Rule 2: exact column name match
          else if (colALower === colBLower && colALower !== "id") {
            confidence = 0.65;
            signal = "name_pattern:exact_match";
          }
          // Rule 3: strip _id/_key suffix match
          else if (
            stripIdSuffix(colA.column_name) !== "" &&
            stripIdSuffix(colA.column_name) === stripIdSuffix(colB.column_name) &&
            (colALower.endsWith("_id") || colALower.endsWith("_key") ||
             colBLower.endsWith("_id") || colBLower.endsWith("_key"))
          ) {
            confidence = 0.5;
            signal = "name_pattern:stripped_suffix";
          }

          if (confidence > 0) {
            const condition = `${tA.tableName}.${colA.column_name} = ${tB.tableName}.${colB.column_name}`;
            addCandidate({
              pathId: pathId(CROSS_SOURCE_ID, tA.tableName, tB.tableName, condition),
              sourceId: CROSS_SOURCE_ID,
              tableA: tA.tableName,
              tableB: tB.tableName,
              joinType: "INNER",
              joinCondition: condition,
              confidence,
              signalSource: signal,
              sqlTemplate: `SELECT * FROM ${tA.tableName} JOIN ${tB.tableName} ON ${condition}`,
              version: 1,
            });
          }
        }
      }
    }
  }

  // ── Signal 3: Embedding similarity ──
  if (embedder && embedder.isReady()) {
    const { Embedder: EmbedderClass } = await import("../semantic/embedder.js");
    for (let i = 0; i < allTableCols.length; i++) {
      for (let j = i + 1; j < allTableCols.length; j++) {
        const tA = allTableCols[i];
        const tB = allTableCols[j];
        if (tA.tableName === tB.tableName) continue;

        const embA = catalog.getEmbedding(tA.sourceId, tA.tableName);
        const embB = catalog.getEmbedding(tB.sourceId, tB.tableName);

        if (embA && embB) {
          const cosine = EmbedderClass.cosineSimilarity(embA.vector, embB.vector);
          if (cosine > 0.7) {
            const embScore = cosine * 0.6;
            const key = [tA.tableName, tB.tableName].sort().join("|");
            const existing = pairCandidates.get(key) ?? [];
            for (const c of existing) {
              if (c.signalSource.startsWith("name_pattern")) {
                const colScore = c.confidence;
                c.confidence = 0.6 * colScore + 0.4 * embScore;
                c.signalSource += "+embedding";
              }
            }
            if (existing.length === 0) {
              addCandidate({
                pathId: pathId(CROSS_SOURCE_ID, tA.tableName, tB.tableName, `embedding:${cosine.toFixed(3)}`),
                sourceId: CROSS_SOURCE_ID,
                tableA: tA.tableName,
                tableB: tB.tableName,
                joinType: "INNER",
                joinCondition: `-- embedding similarity ${cosine.toFixed(3)}, manual verification needed`,
                confidence: embScore,
                signalSource: "embedding",
                sqlTemplate: `-- No auto-generated JOIN: tables are semantically related (cosine=${cosine.toFixed(3)}) but no matching columns found`,
                version: 1,
              });
            }
          }
        }
      }
    }
  }

  // ── Fusion: filter and keep top-3 per pair ──
  const directPaths: JoinPath[] = [];
  for (const [_key, candidates] of pairCandidates) {
    const fkPaths = candidates.filter((c) => c.signalSource === "fk_constraint");
    if (fkPaths.length > 0) {
      directPaths.push(...fkPaths);
      continue;
    }
    const valid = candidates.filter((c) => c.confidence >= 0.4);
    valid.sort((a, b) => b.confidence - a.confidence);
    directPaths.push(...valid.slice(0, 3));
  }

  // ── Indirect paths (1-hop) ──
  const indirectPaths: JoinPath[] = [];
  const directPairSet = new Set<string>();
  for (const p of directPaths) {
    directPairSet.add([p.tableA, p.tableB].sort().join("|"));
  }

  const adjacency = new Map<string, Map<string, JoinPath>>();
  for (const p of directPaths) {
    if (!adjacency.has(p.tableA)) adjacency.set(p.tableA, new Map());
    if (!adjacency.has(p.tableB)) adjacency.set(p.tableB, new Map());
    const existAB = adjacency.get(p.tableA)!.get(p.tableB);
    if (!existAB || p.confidence > existAB.confidence) {
      adjacency.get(p.tableA)!.set(p.tableB, p);
    }
    const existBA = adjacency.get(p.tableB)!.get(p.tableA);
    if (!existBA || p.confidence > existBA.confidence) {
      adjacency.get(p.tableB)!.set(p.tableA, p);
    }
  }

  const tableNames = allTableCols.map((t) => t.tableName);
  const uniqueTableNames = [...new Set(tableNames)];
  for (let i = 0; i < uniqueTableNames.length; i++) {
    for (let j = i + 1; j < uniqueTableNames.length; j++) {
      const tA = uniqueTableNames[i];
      const tB = uniqueTableNames[j];
      const pairKey = [tA, tB].sort().join("|");

      if (directPairSet.has(pairKey)) continue;

      const neighborsA = adjacency.get(tA);
      if (!neighborsA) continue;

      for (const [mid, pathAM] of neighborsA) {
        if (mid === tB) continue;
        const neighborsM = adjacency.get(mid);
        if (!neighborsM) continue;
        const pathMB = neighborsM.get(tB);
        if (!pathMB) continue;

        const conf = Math.min(pathAM.confidence, pathMB.confidence) * 0.8;
        if (conf < 0.4) continue;

        const condition = `${pathAM.joinCondition} AND ${pathMB.joinCondition}`;
        const sqlTemplate = `SELECT * FROM ${tA} JOIN ${mid} ON ${pathAM.joinCondition} JOIN ${tB} ON ${pathMB.joinCondition}`;

        indirectPaths.push({
          pathId: pathId(CROSS_SOURCE_ID, tA, tB, `via:${mid}`),
          sourceId: CROSS_SOURCE_ID,
          tableA: tA,
          tableB: tB,
          joinType: "INNER",
          joinCondition: condition,
          confidence: conf,
          signalSource: `indirect:via_${mid}`,
          sqlTemplate,
          version: 1,
        });
      }
    }
  }

  return [...directPaths, ...indirectPaths];
}
