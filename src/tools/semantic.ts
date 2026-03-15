/**
 * init_semantic / update_semantic tool handlers.
 */

import type { TableSemantic, ColumnSemantic } from "../types.js";
import { CatalogStore } from "../catalog/store.js";
import { QueryEngine } from "../query/engine.js";
import { writeTableSemantic } from "../semantic/layer.js";
import { SemanticIndex } from "../semantic/index.js";
import { getConnectors } from "./connect.js";

/**
 * init_semantic — returns all connected table schemas, sample data, and profiling
 * in a format optimized for the host Agent to infer business semantics.
 */
export async function handleInitSemantic(
  params: Record<string, unknown>,
  catalog: CatalogStore,
  engine: QueryEngine,
): Promise<string> {
  const filterSourceId = (params.source_id ?? params.sourceId) as
    | string
    | undefined;
  const sampleRows = Number(params.sample_rows ?? params.sampleRows ?? 5);

  const allTables = catalog.getTables(filterSourceId);

  if (allTables.length === 0) {
    return JSON.stringify({
      message:
        "No tables found. Please connect a data source first using connect_source.",
      tables: [],
    });
  }

  const result: Array<{
    source: string;
    table: string;
    rowCount: number;
    semanticStatus: string;
    columns: Array<{
      name: string;
      dtype: string;
      nullable: boolean;
      comment: string | null;
      sampleValues: unknown[];
    }>;
    foreignKeys: Array<{ column: string; references: string }>;
    sampleData: Record<string, unknown>[];
  }> = [];

  for (const tableMeta of allTables) {
    const columns = catalog.getColumns(tableMeta.source_id, tableMeta.table_name);

    // Get sample data from DuckDB
    let sampleData: Record<string, unknown>[] = [];
    try {
      sampleData = (await engine.executeRaw(
        `SELECT * FROM "${tableMeta.table_name}" LIMIT ${sampleRows}`,
      )) as Record<string, unknown>[];
    } catch {
      // Table might not be in DuckDB (e.g., MySQL-only)
    }

    // Get foreign keys from connector if available
    const connector = getConnectors().get(tableMeta.source_id);
    let foreignKeys: Array<{ column: string; references: string }> = [];
    if (connector) {
      try {
        const info = await connector.getTableInfo(tableMeta.table_name);
        foreignKeys = info.foreignKeys ?? [];
      } catch {
        // Ignore
      }
    }

    result.push({
      source: tableMeta.source_id,
      table: tableMeta.table_name,
      rowCount: tableMeta.row_count,
      semanticStatus: tableMeta.semantic_status,
      columns: columns.map((c) => ({
        name: c.column_name,
        dtype: c.dtype,
        nullable: c.nullable === 1,
        comment: c.comment,
        sampleValues: c.sample_values ? JSON.parse(c.sample_values) : [],
      })),
      foreignKeys,
      sampleData,
    });
  }

  return JSON.stringify(
    {
      message: `Found ${result.length} table(s) ready for semantic inference. Please analyze each table and call update_semantic with the results.`,
      tables: result,
    },
    (_key, val) => (typeof val === "bigint" ? String(val) : val),
    2,
  );
}

/**
 * update_semantic — writes semantic definitions to YAML and updates the BM25 index.
 */
export function handleUpdateSemantic(
  params: Record<string, unknown>,
  catalog: CatalogStore,
  index: SemanticIndex,
): string {
  const tables = params.tables as Array<Record<string, unknown>>;

  if (!tables || !Array.isArray(tables) || tables.length === 0) {
    throw new Error("tables array is required and must not be empty");
  }

  const results: Array<{ tableName: string; status: string }> = [];

  for (const tableInput of tables) {
    const tableName = (tableInput.table_name ?? tableInput.tableName) as string;

    if (!tableName) {
      results.push({ tableName: "unknown", status: "error: table_name is required" });
      continue;
    }

    // Find the source_id for this table
    let sourceId = (tableInput.source_id ?? tableInput.sourceId) as
      | string
      | undefined;
    if (!sourceId) {
      const allTables = catalog.getTables();
      const match = allTables.find((t) => t.table_name === tableName);
      if (!match) {
        results.push({
          tableName,
          status: `error: table "${tableName}" not found in any connected source`,
        });
        continue;
      }
      sourceId = match.source_id;
    }

    // Build TableSemantic from input
    const semantic: TableSemantic = {
      source: sourceId,
      table: tableName,
      description: tableInput.description as string | undefined,
      businessDomain: (tableInput.business_domain ??
        tableInput.businessDomain) as string | undefined,
      tags: tableInput.tags as string[] | undefined,
      confirmed: false,
      updatedAt: new Date().toISOString(),
      columns: ((tableInput.columns as Array<Record<string, unknown>>) ?? []).map(
        (col): ColumnSemantic => ({
          name: col.name as string,
          semantic: col.semantic as string | undefined,
          role: col.role as ColumnSemantic["role"],
          unit: col.unit as string | undefined,
          aggregation: col.aggregation as string | undefined,
          references: col.references as string | undefined,
          enumValues: col.enum_values as Record<string, string> | undefined ??
            col.enumValues as Record<string, string> | undefined,
          granularity: col.granularity as string[] | undefined,
          confirmed: (col.confirmed as boolean) ?? false,
        }),
      ),
      relations: (
        (tableInput.relations as Array<Record<string, unknown>>) ?? []
      ).map((rel) => ({
        targetTable: (rel.target_table ?? rel.targetTable) as string,
        joinCondition: (rel.join_condition ?? rel.joinCondition) as string,
        relationType: (rel.relation_type ?? rel.relationType) as
          | "one_to_one"
          | "one_to_many"
          | "many_to_one"
          | "many_to_many",
        confirmed: (rel.confirmed as boolean) ?? false,
      })),
      metrics: (
        (tableInput.metrics as Array<Record<string, unknown>>) ?? []
      ).map((m) => ({
        name: m.name as string,
        expression: m.expression as string,
        groupBy: (m.group_by ?? m.groupBy) as string | undefined,
        filter: m.filter as string | undefined,
      })),
    };

    // Write YAML
    writeTableSemantic(sourceId, tableName, semantic);

    // Update BM25 index
    index.indexTable(sourceId, tableName, semantic);

    // Update semantic status in catalog
    catalog.updateSemanticStatus(sourceId, tableName, "inferred");

    results.push({ tableName, status: "updated" });
  }

  const updated = results.filter((r) => r.status === "updated").length;
  const errors = results.filter((r) => r.status.startsWith("error")).length;

  return JSON.stringify(
    {
      message: `Semantic layer updated: ${updated} table(s) written, ${errors} error(s).`,
      indexedCount: index.count(),
      results,
    },
    null,
    2,
  );
}
