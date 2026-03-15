/**
 * search_tables — combines BM25 index lookup with full semantic YAML loading.
 */

import type { TableSemantic } from "../types.js";
import { SemanticIndex } from "./index.js";
import { readTableSemantic } from "./layer.js";

export interface SearchResult {
  sourceId: string;
  tableName: string;
  rank: number;
  semantic: TableSemantic | null;
}

/**
 * Search for tables matching a natural language query.
 * Returns full semantic definitions for each match.
 */
export function searchTables(
  index: SemanticIndex,
  query: string,
  topK = 5,
): SearchResult[] {
  const matches = index.search(query, topK);

  return matches.map((m) => ({
    sourceId: m.sourceId,
    tableName: m.tableName,
    rank: m.rank,
    semantic: readTableSemantic(m.sourceId, m.tableName),
  }));
}
