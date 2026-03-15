/**
 * search_tables tool handler.
 */

import { SemanticIndex } from "../semantic/index.js";
import { searchTables, type SearchResult } from "../semantic/search.js";

/**
 * search_tables — returns matching tables with full semantic info.
 */
export function handleSearchTables(
  params: Record<string, unknown>,
  index: SemanticIndex,
): string {
  const query = params.query as string;
  const topK = Number(params.top_k ?? params.topK ?? 5);

  if (!query) {
    throw new Error("query is required");
  }

  const results = searchTables(index, query, topK);

  if (results.length === 0) {
    return JSON.stringify({
      message: `No tables found matching "${query}". Try different keywords, or check if semantic layer has been initialized (call init_semantic + update_semantic first).`,
      results: [],
    });
  }

  return JSON.stringify(
    {
      message: `Found ${results.length} table(s) matching "${query}"`,
      results: results.map((r) => ({
        sourceId: r.sourceId,
        tableName: r.tableName,
        relevanceRank: r.rank,
        description: r.semantic?.description,
        businessDomain: r.semantic?.businessDomain,
        tags: r.semantic?.tags,
        columns: r.semantic?.columns?.map((c) => ({
          name: c.name,
          semantic: c.semantic,
          role: c.role,
          unit: c.unit,
        })),
        relations: r.semantic?.relations?.map((rel) => ({
          targetTable: rel.targetTable,
          joinCondition: rel.joinCondition,
          relationType: rel.relationType,
        })),
        metrics: r.semantic?.metrics,
      })),
    },
    null,
    2,
  );
}
