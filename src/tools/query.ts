import type { QueryFormat } from "../types.js";
import { QueryEngine } from "../query/engine.js";
import { QueryRouter } from "../query/router.js";
import { formatQueryResult } from "../query/formatter.js";

/**
 * query tool handler.
 * Routes through QueryRouter for MySQL direct execution when possible,
 * otherwise falls back to DuckDB via QueryEngine.
 */
export async function handleQuery(
  params: Record<string, unknown>,
  engine: QueryEngine,
  router?: QueryRouter,
): Promise<string> {
  const sql = params.sql as string;
  const maxRows = (params.maxRows as number) ?? 100;
  const format = (params.format as QueryFormat) ?? "markdown";

  if (!sql) {
    throw new Error("sql parameter is required");
  }

  const result = router
    ? await router.route(sql, maxRows)
    : await engine.execute(sql, maxRows);

  return formatQueryResult(result, format);
}
