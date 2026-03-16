/**
 * Shared type definitions for Lucid MCP Server.
 */

// ── Data Source Types ──

export type SourceType = "excel" | "csv" | "mysql" | "postgresql";

export interface ExcelSourceConfig {
  type: "excel";
  path: string;
  sheets?: string[];
}

export interface CsvSourceConfig {
  type: "csv";
  path: string;
}

export interface MySQLSourceConfig {
  type: "mysql";
  host: string;
  port?: number;
  database: string;
  username: string;
  password: string;
}

export interface PostgreSQLSourceConfig {
  type: "postgresql";
  host: string;
  port?: number;
  database: string;
  username: string;
  password: string;
  schema?: string;
}

export type SourceConfig = ExcelSourceConfig | CsvSourceConfig | MySQLSourceConfig | PostgreSQLSourceConfig;

// ── Schema Types ──

export interface ColumnInfo {
  name: string;
  dtype: string;
  nullable: boolean;
  comment: string | null;
  sampleValues: unknown[];
}

export interface ForeignKey {
  column: string;
  references: string;
}

export interface TableInfo {
  name: string;
  source: string;
  rowCount: number;
  columns: ColumnInfo[];
  foreignKeys?: ForeignKey[];
}

// ── Semantic Types ──

export type SemanticStatus = "not_initialized" | "inferred" | "confirmed";

export type ColumnRole =
  | "primary_key"
  | "foreign_key"
  | "timestamp"
  | "measure"
  | "dimension";

export interface ColumnSemantic {
  name: string;
  semantic?: string;
  role?: ColumnRole;
  unit?: string;
  aggregation?: string;
  references?: string;
  enumValues?: Record<string, string>;
  granularity?: string[];
  confirmed: boolean;
}

export interface RelationSemantic {
  targetTable: string;
  joinCondition: string;
  relationType: "one_to_one" | "one_to_many" | "many_to_one" | "many_to_many";
  confirmed: boolean;
}

export interface MetricDefinition {
  name: string;
  expression: string;
  groupBy?: string;
  filter?: string;
}

export interface TableSemantic {
  source: string;
  table: string;
  description?: string;
  businessDomain?: string;
  tags?: string[];
  confirmed: boolean;
  updatedAt: string;
  columns: ColumnSemantic[];
  relations?: RelationSemantic[];
  metrics?: MetricDefinition[];
}

// ── Query Types ──

export type QueryFormat = "json" | "markdown" | "csv";

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  truncated: boolean;
}

// ── Config Types ──

export interface LucidConfig {
  server: {
    name: string;
    version: string;
    transport: "stdio";
  };
  query: {
    maxRows: number;
    timeoutSeconds: number;
    memoryLimit: string;
  };
  semantic: {
    storePath: string;
  };
  catalog: {
    dbPath: string;
    autoProfile: boolean;
  };
  logging: {
    level: "debug" | "info" | "warn" | "error";
  };
}
