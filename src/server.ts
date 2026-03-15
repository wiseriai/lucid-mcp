import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { CatalogStore } from "./catalog/store.js";
import { QueryEngine } from "./query/engine.js";
import { QueryRouter } from "./query/router.js";
import { SemanticIndex } from "./semantic/index.js";
import { handleConnectSource, handleListTables } from "./tools/connect.js";
import { handleQuery } from "./tools/query.js";
import { handleDescribeTable } from "./tools/describe.js";
import { handleProfileData } from "./tools/profile.js";
import { handleInitSemantic, handleUpdateSemantic } from "./tools/semantic.js";
import { handleSearchTables } from "./tools/search.js";
import { getConfig } from "./config.js";

/**
 * Create and configure the Lucid MCP Server.
 */
export function createServer(): McpServer {
  const config = getConfig();
  const catalog = new CatalogStore();
  const engine = new QueryEngine();
  const router = new QueryRouter(engine);
  const semanticIndex = new SemanticIndex();

  const server = new McpServer({
    name: config.server.name,
    version: config.server.version,
  });

  // ── Tool: connect_source ──────────────────────────────────────────────────
  server.tool(
    "connect_source",
    "Connect a data source (Excel, CSV, or MySQL). Automatically collects schema and basic profiling.",
    {
      type: z.enum(["excel", "csv", "mysql"]).describe("Data source type"),
      path: z.string().optional().describe("File path for Excel/CSV sources"),
      sheets: z.array(z.string()).optional().describe("Sheet names to load (Excel only, default: all)"),
      host: z.string().optional().describe("MySQL host"),
      port: z.number().optional().describe("MySQL port (default: 3306)"),
      database: z.string().optional().describe("MySQL database name"),
      username: z.string().optional().describe("MySQL username"),
      password: z.string().optional().describe("MySQL password"),
    },
    async (params) => {
      try {
        const result = await handleConnectSource(params, catalog, engine, router);
        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              sourceId: result.sourceId,
              message: `Connected successfully. Found ${result.tables.length} table(s): ${result.tables.map((t) => t.name).join(", ")}`,
              tables: result.tables.map((t) => ({
                name: t.name,
                rowCount: t.rowCount,
                columnCount: t.columns.length,
                columns: t.columns.map((c) => ({ name: c.name, dtype: c.dtype, comment: c.comment })),
              })),
            }, (_key, val) => typeof val === "bigint" ? String(val) : val, 2),
          }],
        };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error connecting source: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── Tool: list_tables ─────────────────────────────────────────────────────
  server.tool(
    "list_tables",
    "List all connected data tables with metadata (row count, column count, semantic status).",
    {
      source_id: z.string().optional().describe("Filter by source ID"),
    },
    async (params) => {
      try {
        const tables = await handleListTables({ sourceId: params.source_id }, catalog);
        return {
          content: [{ type: "text" as const, text: JSON.stringify(tables, null, 2) }],
        };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error listing tables: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── Tool: describe_table ──────────────────────────────────────────────────
  server.tool(
    "describe_table",
    "View the detailed structure, column types, and business semantics of a specific table. Optionally includes sample data.",
    {
      table_name: z.string().describe("Name of the table to describe"),
      source_id: z.string().optional().describe("Source ID (optional, auto-detected if omitted)"),
      include_sample: z.boolean().optional().default(true).describe("Include sample rows (default: true)"),
      sample_rows: z.number().optional().default(5).describe("Number of sample rows (default: 5)"),
    },
    async (params) => {
      try {
        const result = await handleDescribeTable(params, catalog, engine);
        return { content: [{ type: "text" as const, text: result }] };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error describing table: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── Tool: query ───────────────────────────────────────────────────────────
  server.tool(
    "query",
    "Execute a read-only SQL query (SELECT only). Returns results in JSON, markdown, or CSV format.",
    {
      sql: z.string().describe("SQL SELECT statement to execute"),
      max_rows: z.number().optional().default(100).describe("Maximum rows to return (default: 100)"),
      format: z.enum(["json", "markdown", "csv"]).optional().default("markdown").describe("Output format (default: markdown)"),
    },
    async (params) => {
      try {
        const result = await handleQuery({ sql: params.sql, maxRows: params.max_rows, format: params.format }, engine, router);
        return { content: [{ type: "text" as const, text: result }] };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error executing query: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── Tool: profile_data ────────────────────────────────────────────────────
  server.tool(
    "profile_data",
    "Run a deep data profile on a table using DuckDB SUMMARIZE. Returns stats: null rate, distinct count, min/max/avg, quartiles.",
    {
      table_name: z.string().describe("Name of the table to profile"),
      source_id: z.string().optional().describe("Source ID (optional, auto-detected if omitted)"),
    },
    async (params) => {
      try {
        const result = await handleProfileData(params, catalog, engine);
        return { content: [{ type: "text" as const, text: result }] };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error profiling data: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── Tool: init_semantic ───────────────────────────────────────────────────
  server.tool(
    "init_semantic",
    "Return all connected table schemas, sample data, and profiling summaries for the host Agent to infer business semantics. After inference, call update_semantic to save results.",
    {
      source_id: z.string().optional().describe("Filter by source ID (optional)"),
      sample_rows: z.number().optional().default(5).describe("Sample rows per table (default: 5)"),
    },
    async (params) => {
      try {
        const result = await handleInitSemantic(params, catalog, engine);
        return { content: [{ type: "text" as const, text: result }] };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error initializing semantic: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── Tool: update_semantic ─────────────────────────────────────────────────
  server.tool(
    "update_semantic",
    "Write or update business semantic definitions for tables. Saves to YAML files and automatically updates the BM25 search index. Supports batch updates for multiple tables.",
    {
      tables: z.array(z.object({
        table_name: z.string().describe("Table name"),
        description: z.string().optional().describe("Business description of the table"),
        business_domain: z.string().optional().describe("Business domain (e.g., '电商/交易')"),
        tags: z.array(z.string()).optional().describe("Searchable tags"),
        columns: z.array(z.object({
          name: z.string(),
          semantic: z.string().optional().describe("Business meaning of the column"),
          role: z.enum(["primary_key", "foreign_key", "timestamp", "measure", "dimension"]).optional(),
          unit: z.string().optional().describe("Unit of measure (e.g., 'CNY', 'USD')"),
          aggregation: z.string().optional().describe("Default aggregation (e.g., 'sum', 'avg')"),
          references: z.string().optional().describe("Foreign key reference (e.g., 'users.id')"),
          enum_values: z.record(z.string()).optional().describe("Enum value mappings"),
          granularity: z.array(z.string()).optional().describe("Time granularity options"),
          confirmed: z.boolean().optional().default(false),
        })).optional(),
        relations: z.array(z.object({
          target_table: z.string(),
          join_condition: z.string(),
          relation_type: z.enum(["one_to_one", "one_to_many", "many_to_one", "many_to_many"]),
          confirmed: z.boolean().optional().default(false),
        })).optional(),
        metrics: z.array(z.object({
          name: z.string(),
          expression: z.string(),
          group_by: z.string().optional(),
          filter: z.string().optional(),
        })).optional(),
      })).describe("Array of table semantic definitions"),
    },
    async (params) => {
      try {
        const result = handleUpdateSemantic(params, catalog, semanticIndex);
        return { content: [{ type: "text" as const, text: result }] };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error updating semantic: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── Tool: search_tables ───────────────────────────────────────────────────
  server.tool(
    "search_tables",
    "Search the semantic layer using natural language keywords to find relevant tables and fields. Returns full semantic definitions including column meanings, JOIN relations, and metric definitions. Use this before generating SQL to understand which tables to query.",
    {
      query: z.string().describe("Natural language keywords or question (e.g., '上月销售额 客户')"),
      top_k: z.number().optional().default(5).describe("Top K most relevant tables to return (default: 5)"),
    },
    async (params) => {
      try {
        const result = handleSearchTables(params, semanticIndex);
        return { content: [{ type: "text" as const, text: result }] };
      } catch (error) {
        return {
          content: [{ type: "text" as const, text: `Error searching tables: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true,
        };
      }
    },
  );

  // ── MCP Prompt: infer_semantic ────────────────────────────────────────────
  server.prompt(
    "infer_semantic",
    "Guide the Agent to infer business semantics from connected data sources. Call init_semantic first, then use this prompt to analyze the schema.",
    [
      { name: "source_id", description: "Source ID to focus on (optional)", required: false },
    ],
    async (_params) => {
      return {
        messages: [
          {
            role: "user" as const,
            content: {
              type: "text" as const,
              text: `请根据以下数据库表的结构和样本数据，推断每张表和每个字段的业务含义。

请先调用 init_semantic 获取所有表的 schema 和样本数据，然后按以下要求分析：

1. **表级别**：为每张表生成业务描述（description）、所属业务域（business_domain）和标签（tags）
2. **字段级别**：为每个字段推断：
   - 语义含义（semantic）
   - 角色（role）：primary_key / foreign_key / timestamp / measure / dimension
   - 单位（unit）：如 CNY、USD、个、次
   - 默认聚合方式（aggregation）：sum / avg / count / max / min
3. **枚举字段**：识别枚举字段并推断每个值的业务含义（enum_values）
4. **表间关系**：基于字段名和外键信息推断 JOIN 关系（relations）
5. **业务指标**：定义常用指标如日订单量、GMV 等（metrics）

推断完成后，请调用 update_semantic 将全部结果批量写入语义层。

注意：
- 即使不确定也请给出最佳推测（confirmed 设为 false）
- 字段名可能是缩写或编码，请尽量推断真实含义
- 中文和英文语义都可以`,
            },
          },
        ],
      };
    },
  );

  return server;
}
