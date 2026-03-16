import type { Connector } from "./base.js";
import type { ColumnInfo, ForeignKey, TableInfo } from "../types.js";
import pg from "pg";

interface PostgresConfig {
  host: string;
  port?: number;
  database: string;
  username: string;
  password: string;
  schema?: string;
}

export class PostgresConnector implements Connector {
  readonly sourceType = "postgresql";
  sourceId = "";
  private config: PostgresConfig | null = null;
  private client: pg.Client | null = null;
  private schema = "public";

  async connect(config: Record<string, unknown>): Promise<void> {
    this.config = config as unknown as PostgresConfig;
    this.schema = this.config.schema ?? "public";
    this.sourceId = `postgresql:${this.config.database}`;

    this.client = new pg.Client({
      host: this.config.host,
      port: this.config.port ?? 5432,
      database: this.config.database,
      user: this.config.username,
      password: this.config.password,
    });

    await this.client.connect();
  }

  async listTables(): Promise<string[]> {
    const result = await this.client!.query(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'
       ORDER BY table_name`,
      [this.schema],
    );
    return result.rows.map((row: { table_name: string }) => row.table_name);
  }

  async getTableInfo(table: string): Promise<TableInfo> {
    const columns = await this.getColumns(table);
    const foreignKeys = await this.getForeignKeys(table);

    const countResult = await this.client!.query(
      `SELECT COUNT(*) as cnt FROM "${this.schema}"."${table}"`,
    );
    const rowCount = parseInt(countResult.rows[0].cnt, 10);

    return {
      name: table,
      source: this.sourceId,
      rowCount,
      columns,
      foreignKeys: foreignKeys.length > 0 ? foreignKeys : undefined,
    };
  }

  async getSampleData(table: string, limit = 5): Promise<Record<string, unknown>[]> {
    const result = await this.client!.query(
      `SELECT * FROM "${this.schema}"."${table}" LIMIT $1`,
      [limit],
    );
    return result.rows;
  }

  async registerToDuckDB(_db: unknown): Promise<string[]> {
    // PostgreSQL data is queried directly via pg.
    // For cross-source JOIN, data would be loaded into DuckDB (future).
    return this.listTables();
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.end();
      this.client = null;
    }
  }

  getClient(): pg.Client | null {
    return this.client;
  }

  async executeQuery(sql: string): Promise<{ columns: string[]; rows: Record<string, unknown>[] }> {
    const result = await this.client!.query(sql);
    const columns = result.fields.map((f) => f.name);
    return { columns, rows: result.rows };
  }

  private async getColumns(table: string): Promise<ColumnInfo[]> {
    const result = await this.client!.query(
      `SELECT c.column_name, c.data_type, c.is_nullable,
              pgd.description as column_comment
       FROM information_schema.columns c
       LEFT JOIN pg_catalog.pg_statio_all_tables st
         ON st.schemaname = c.table_schema AND st.relname = c.table_name
       LEFT JOIN pg_catalog.pg_description pgd
         ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
       WHERE c.table_schema = $1 AND c.table_name = $2
       ORDER BY c.ordinal_position`,
      [this.schema, table],
    );

    const columns: ColumnInfo[] = [];
    for (const col of result.rows) {
      const sampleResult = await this.client!.query(
        `SELECT DISTINCT "${col.column_name}" FROM "${this.schema}"."${table}"
         WHERE "${col.column_name}" IS NOT NULL LIMIT 5`,
      );
      columns.push({
        name: col.column_name,
        dtype: col.data_type,
        nullable: col.is_nullable === "YES",
        comment: col.column_comment || null,
        sampleValues: sampleResult.rows.map(
          (r: Record<string, unknown>) => r[col.column_name],
        ),
      });
    }
    return columns;
  }

  private async getForeignKeys(table: string): Promise<ForeignKey[]> {
    const result = await this.client!.query(
      `SELECT kcu.column_name,
              ccu.table_name AS referenced_table_name,
              ccu.column_name AS referenced_column_name
       FROM information_schema.key_column_usage kcu
       JOIN information_schema.referential_constraints rc
         ON kcu.constraint_name = rc.constraint_name
         AND kcu.constraint_schema = rc.constraint_schema
       JOIN information_schema.constraint_column_usage ccu
         ON rc.unique_constraint_name = ccu.constraint_name
         AND rc.unique_constraint_schema = ccu.constraint_schema
       WHERE kcu.table_schema = $1 AND kcu.table_name = $2`,
      [this.schema, table],
    );

    return result.rows.map((r: { column_name: string; referenced_table_name: string; referenced_column_name: string }) => ({
      column: r.column_name,
      references: `${r.referenced_table_name}.${r.referenced_column_name}`,
    }));
  }
}
