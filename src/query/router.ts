import type { QueryResult } from "../types.js";
import type { Connector } from "../connectors/base.js";
import { MySQLConnector } from "../connectors/mysql.js";
import { PostgresConnector } from "../connectors/postgres.js";
import { QueryEngine } from "./engine.js";
import { checkSqlSafety } from "./safety.js";

/**
 * Query router — decides whether to execute via MySQL/PostgreSQL directly or DuckDB.
 *
 * Routing logic:
 * - Pure DuckDB tables (Excel/CSV) → DuckDB engine
 * - Pure MySQL query (all referenced tables from same MySQL source) → MySQL direct
 * - Pure PostgreSQL query (all referenced tables from same PG source) → PostgreSQL direct
 * - Mixed / unknown → DuckDB (data already loaded at connect time)
 */
export class QueryRouter {
  private connectors: Map<string, Connector> = new Map();
  private engine: QueryEngine;
  // Map of table_name → source_id for quick routing lookup
  private tableSourceMap: Map<string, string> = new Map();

  constructor(engine: QueryEngine) {
    this.engine = engine;
  }

  registerConnector(sourceId: string, connector: Connector, tables: string[]): void {
    this.connectors.set(sourceId, connector);
    for (const t of tables) {
      this.tableSourceMap.set(t.toLowerCase(), sourceId);
    }
  }

  /**
   * Route and execute a query.
   */
  async route(sql: string, maxRows?: number): Promise<QueryResult> {
    const check = checkSqlSafety(sql);
    if (!check.safe) {
      throw new Error(`SQL safety check failed: ${check.reason}`);
    }

    const directSource = this.detectDirectSource(sql);
    if (directSource) {
      return this.executeOnDirectSource(directSource, sql, maxRows);
    }

    return this.engine.execute(sql, maxRows);
  }

  getEngine(): QueryEngine {
    return this.engine;
  }

  /**
   * Detect if all referenced tables in the SQL belong to the same direct-query source
   * (MySQL or PostgreSQL). Returns the source_id if yes, null otherwise.
   */
  private detectDirectSource(sql: string): string | null {
    // Extract table names from SQL (simple regex, covers common patterns)
    const tablePattern = /(?:FROM|JOIN)\s+[`"]?(\w+)[`"]?/gi;
    const mentioned: string[] = [];
    let match: RegExpExecArray | null;

    while ((match = tablePattern.exec(sql)) !== null) {
      mentioned.push(match[1].toLowerCase());
    }

    if (mentioned.length === 0) return null;

    // Check if all mentioned tables belong to the same direct-query source
    let candidateSource: string | null = null;
    for (const tableName of mentioned) {
      const sourceId = this.tableSourceMap.get(tableName);
      if (!sourceId) return null; // Unknown table, fall back to DuckDB
      if (!sourceId.startsWith("mysql:") && !sourceId.startsWith("postgresql:")) return null;

      if (candidateSource === null) {
        candidateSource = sourceId;
      } else if (candidateSource !== sourceId) {
        return null; // Tables from different sources — need DuckDB
      }
    }

    return candidateSource;
  }

  /**
   * Execute a query directly on the source database (MySQL or PostgreSQL)
   * and wrap result in QueryResult format.
   */
  private async executeOnDirectSource(
    sourceId: string,
    sql: string,
    maxRows?: number,
  ): Promise<QueryResult> {
    const connector = this.connectors.get(sourceId);
    if (!connector || (!(connector instanceof MySQLConnector) && !(connector instanceof PostgresConnector))) {
      // Fallback to DuckDB
      return this.engine.execute(sql, maxRows);
    }

    const limit = maxRows ?? 1000;
    // Append LIMIT if not already present
    const limitedSql = /\bLIMIT\s+\d+\s*$/i.test(sql.trim())
      ? sql
      : `${sql.trim()} LIMIT ${limit}`;

    const { columns, rows } = await connector.executeQuery(limitedSql);

    return {
      columns,
      rows,
      rowCount: rows.length,
      truncated: rows.length === limit,
    };
  }
}
