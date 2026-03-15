import type { Connector } from "../connectors/base.js";
import type { SourceConfig, TableInfo } from "../types.js";
import { ExcelConnector } from "../connectors/excel.js";
import { CsvConnector } from "../connectors/csv.js";
import { MySQLConnector } from "../connectors/mysql.js";
import { CatalogStore } from "../catalog/store.js";
import { QueryEngine } from "../query/engine.js";
import { QueryRouter } from "../query/router.js";
import { collectSchema } from "../catalog/schema.js";

/**
 * Global connector registry — shared across tools.
 */
const connectors = new Map<string, Connector>();

export function getConnectors(): Map<string, Connector> {
  return connectors;
}

export function getConnector(sourceId: string): Connector | undefined {
  return connectors.get(sourceId);
}

/**
 * connect_source tool handler.
 */
export async function handleConnectSource(
  params: Record<string, unknown>,
  catalog: CatalogStore,
  engine?: QueryEngine,
  router?: QueryRouter,
): Promise<{ sourceId: string; tables: TableInfo[] }> {
  const config = params as unknown as SourceConfig;

  let connector: Connector;
  switch (config.type) {
    case "excel":
      connector = new ExcelConnector();
      break;
    case "csv":
      connector = new CsvConnector();
      break;
    case "mysql":
      connector = new MySQLConnector();
      break;
    default:
      throw new Error(`Unsupported source type: ${(config as { type: string }).type}`);
  }

  await connector.connect(params);

  const sourceId = connector.sourceId;
  connectors.set(sourceId, connector);

  // Persist source to catalog
  catalog.upsertSource(sourceId, connector.sourceType, params);

  // Collect schema
  const tables = await collectSchema(connector, catalog);

  // Register tables into the shared query engine DuckDB (for Excel/CSV)
  if (engine) {
    await connector.registerToDuckDB(engine.getDatabase());
  }

  // Register connector in query router for routing decisions
  if (router) {
    router.registerConnector(sourceId, connector, tables.map((t) => t.name));
  }

  return { sourceId, tables };
}

/**
 * list_tables tool handler.
 */
export async function handleListTables(
  params: Record<string, unknown>,
  catalog: CatalogStore,
): Promise<
  Array<{
    source_id: string;
    table_name: string;
    row_count: number;
    column_count: number;
    semantic_status: string;
  }>
> {
  const sourceId = params.sourceId as string | undefined;
  return catalog.getTables(sourceId);
}
