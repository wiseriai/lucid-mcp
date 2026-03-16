# lucid-mcp

**AI-native data analysis agent as an MCP Server.**

Connect your Excel files, CSVs, MySQL, and PostgreSQL databases. Understand business semantics. Query with natural language.

```jsonc
// Claude Desktop / Cursor config
{
  "mcpServers": {
    "lucid": {
      "command": "npx",
      "args": ["@wiseria/lucid-mcp"]
    }
  }
}
```

No API key required. No LLM inside the server. Just plug in and ask questions.

---

## What it does

Lucid MCP gives your AI assistant (Claude, Cursor, etc.) structured access to your business data:

| Tool | What it does |
|------|-------------|
| `connect_source` | Connect Excel / CSV / MySQL / PostgreSQL. Auto-collects schema + profiling. |
| `list_tables` | List all connected tables with row counts and semantic status. |
| `describe_table` | View column types, sample data, and business semantics. |
| `profile_data` | Deep stats: null rate, distinct count, min/max, quartiles. |
| `init_semantic` | Export schema + samples for LLM to infer business meaning. |
| `update_semantic` | Save semantic definitions (YAML) + update search index. |
| `search_tables` | Natural language search → relevant tables + JOIN hints + metrics. |
| `query` | Execute read-only SQL (SELECT only). Returns markdown/JSON/CSV. |

---

## How it works

```
You: "上个月哪个客户下单金额最多？"

Claude:
  1. search_tables("上月 销售 客户")
     → orders 表 (有 Sales 字段、Customer Name、Order Date)

  2. 生成 SQL:
     SELECT "Customer Name", SUM("Sales") as total
     FROM orders
     WHERE "Order Date" >= '2024-02-01'
       AND "Order Date" < '2024-03-01'
     GROUP BY "Customer Name"
     ORDER BY total DESC
     LIMIT 10

  3. query(sql) → 返回结果表格
  4. 解读结果给你
```

**Design principle: Server has no LLM.** All semantic inference and SQL generation is done by the host agent. The server handles deterministic operations only — connecting, cataloging, indexing, querying.

---

## Supported Platforms

| Platform | Status | Config |
|----------|--------|--------|
| Claude Desktop | ✅ Verified | See below |
| Cursor | ✅ Native MCP support | Same config format |
| OpenClaw | ✅ Native MCP support | Same config format |
| Windsurf | ✅ Native MCP support | Same config format |
| Continue.dev | ✅ Native MCP support | Same config format |

---

## Quick Start

### 1. Add to Claude Desktop

**Claude Desktop** — Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "lucid": {
      "command": "npx",
      "args": ["@wiseria/lucid-mcp"]
    }
  }
}
```

**Cursor** — Edit `.cursor/mcp.json` in your project (or global `~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "lucid": {
      "command": "npx",
      "args": ["@wiseria/lucid-mcp"]
    }
  }
}
```

**OpenClaw** — Add to your OpenClaw config:

```json
{
  "plugins": {
    "mcp": {
      "servers": {
        "lucid": {
          "command": "npx",
          "args": ["@wiseria/lucid-mcp"]
        }
      }
    }
  }
}
```

Restart the host application after editing config.

### 2. Connect a data source

Ask Claude:

> "Connect my Excel file at /Users/me/sales.xlsx"

Claude will call `connect_source` and report back the tables it found.

### 3. Initialize semantics (optional but recommended)

Ask Claude:

> "Initialize the semantic layer for my data"

Claude will call `init_semantic` to get the schema, infer business meanings for each table and column, then call `update_semantic` to save them. After this, natural language search works much better.

### 4. Start asking questions

> "Which product category had the highest profit margin last quarter?"
> "Show me the top 10 customers by revenue"
> "What's the average order value by region?"

---

## Supported Data Sources

| Type | Format | Notes |
|------|--------|-------|
| Excel | `.xlsx`, `.xls` | Multiple sheets supported |
| CSV | `.csv` | Auto-detects encoding and delimiter |
| MySQL | MySQL 5.7+ / 8.0+ | Reads foreign keys and column comments |
| PostgreSQL | PostgreSQL 12+ | Reads foreign keys and column comments via `pg_description` |

---

## Semantic Layer

Lucid stores business semantics as YAML files in `./semantic_store/`. These are:

- **Human-readable** — edit them directly if needed
- **Git-friendly** — commit and version your semantic definitions
- **LLM-agnostic** — switching from Claude to GPT doesn't lose your semantic layer

Example:
```yaml
source: "csv:orders.csv"
table: orders
description: "订单记录，包含销售额、折扣、利润等关键商业指标"
businessDomain: "电商/交易"
tags: ["核心表", "财务", "订单"]

columns:
  - name: Sales
    semantic: "订单销售额"
    role: measure
    unit: CNY
    aggregation: sum

  - name: Order Date
    semantic: "下单时间"
    role: timestamp
    granularity: [day, month, year]

metrics:
  - name: "总销售额"
    expression: "SUM(Sales)"
```

---

## Configuration

Optional config file `lucid.config.yaml` in your working directory:

```yaml
query:
  maxRows: 1000        # Max rows per query (default: 1000)
  timeoutSeconds: 30   # Query timeout (default: 30)

semantic:
  storePath: ./semantic_store   # Where to save YAML files

catalog:
  dbPath: ./lucid-catalog.db   # SQLite metadata cache
```

---

## Security

- **Read-only**: Only `SELECT` statements are allowed. `INSERT`, `UPDATE`, `DELETE`, `DROP`, and all DDL are blocked.
- **No credentials stored**: Database passwords are never written to disk.
- **Local only**: All data stays on your machine. Nothing is sent to external services.

---

## Development

```bash
git clone https://github.com/wiseriai/lucid-mcp
cd lucid-mcp
npm install
npm run build
npm run dev    # Run with tsx (no build step)
npm test       # Run e2e tests
```

---

## Roadmap

- [x] Sprint 1: Excel / CSV / MySQL connectors, DuckDB query engine, SQL safety
- [x] Sprint 2: Semantic layer (YAML), BM25 search index, natural language routing
- [x] Sprint 3: Query routing (MySQL direct), npm publish
- [ ] V1: Embedding-based hybrid search (BM25 + vector)
- [ ] V1: Parquet / large file support
- [ ] Commercial: Multi-tenancy, authentication, hosted version

---

## License

MIT
