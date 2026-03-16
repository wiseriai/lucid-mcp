# lucid-mcp

**AI-native data analysis agent as an MCP Server.**

Connect your Excel files, CSVs, MySQL, and PostgreSQL databases. Understand business semantics. Query with natural language.

No API key required. No LLM inside the server. Just plug in and ask questions.

---

## Installation & Setup (for AI Agents)

> **If you are an AI agent** reading this README, follow the steps below to install and configure Lucid MCP for the user. Do not ask the user to run commands — do it yourself.

### Step 1: Verify Node.js

Run `node -v` to confirm Node.js 18+ is installed. If not, inform the user they need Node.js 18+ before proceeding.

### Step 2: Install globally

```bash
npm install -g @wiseria/lucid-mcp
```

This makes the `lucid-mcp` command available system-wide. Alternatively, use `npx @wiseria/lucid-mcp` (downloads on first run, no install needed).

### Step 3: Add to MCP config

Detect which platform the user is on and write the config:

**Claude Desktop**
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

**Cursor**
- Project: `.cursor/mcp.json`
- Global: `~/.cursor/mcp.json`

**OpenClaw**
- `~/.openclaw/openclaw.json` → `mcpServers` section

**Windsurf**
- `~/.windsurf/mcp.json`

**Continue.dev**
- `~/.continue/config.json` → `experimental.modelContextProtocolServers`

Config content (same for all except Continue.dev):

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

For Continue.dev:
```json
{
  "experimental": {
    "modelContextProtocolServers": [
      {
        "transport": {
          "type": "stdio",
          "command": "npx",
          "args": ["@wiseria/lucid-mcp"]
        }
      }
    ]
  }
}
```

To enable embedding hybrid search (optional, downloads ~460MB model on first run):
```json
{
  "mcpServers": {
    "lucid": {
      "command": "npx",
      "args": ["@wiseria/lucid-mcp"],
      "env": { "LUCID_EMBEDDING_ENABLED": "true" }
    }
  }
}
```

### Step 4: Restart the host application

After writing the config, tell the user to restart Claude Desktop / Cursor / etc. for the MCP server to load.

### Step 5: Verify

Once restarted, call `get_overview()` to confirm the server is running. An empty overview (no sources) is normal for first launch.

---

## Data Directory

All data is stored in `~/.lucid-mcp/`:

| Path | Purpose |
|------|---------|
| `lucid-catalog.db` | SQLite metadata catalog (schema, profiling, search index) |
| `semantic_store/` | Semantic YAML definitions (human-readable, Git-friendly) |
| `models/` | Downloaded embedding model (only if embedding enabled) |

Override with `LUCID_DATA_DIR` environment variable.

---

## Tools (11)

| Tool | What it does |
|------|-------------|
| `get_overview` | **Start here.** Shows all connected sources, tables, row counts, semantic status. |
| `connect_source` | Connect Excel / CSV / MySQL / PostgreSQL. Auto-collects schema + profiling. |
| `list_tables` | List all connected tables with row counts and semantic status. |
| `describe_table` | View column types, sample data, and business semantics. |
| `profile_data` | Deep stats: null rate, distinct count, min/max, quartiles. |
| `init_semantic` | Export schema + samples for LLM to infer business meaning. |
| `update_semantic` | Save semantic definitions (YAML) + update search index. |
| `search_tables` | Natural language search → relevant tables + JOIN hints + metrics. |
| `get_join_paths` | Discover JOIN paths between two tables (direct + indirect via 1 hop). |
| `get_business_domains` | Auto-discovered business domains via hierarchical clustering. |
| `query` | Execute read-only SQL (SELECT only). Returns markdown/JSON/CSV. |

---

## Workflow

### First time with a data source

```
1. get_overview()                          → check current state
2. connect_source(type, path/host/...)     → connect data
3. init_semantic()                         → get schema for inference
4. update_semantic(tables=[...])           → save business meanings
5. search_tables("用户的问题")              → find relevant tables
6. get_join_paths(table_a, table_b)        → discover how to JOIN
7. query(sql="SELECT ...")                 → execute and return results
```

### Returning (server auto-restores previous connections)

```
1. get_overview()                          → see what's already connected
2. search_tables("用户的问题")              → find relevant tables
3. query(sql="SELECT ...")                 → execute
```

---

## How it works

```
You: "上个月哪个客户下单金额最多？"

Agent:
  1. search_tables("上月 销售 客户")
     → orders 表 (有 Sales 字段、Customer Name、Order Date)
     → suggestedJoins, suggestedMetricSqls

  2. get_join_paths("orders", "customers")
     → JOIN orders ON orders.customer_id = customers.id

  3. query(sql)
     → 返回结果表格

  4. 解读结果给用户
```

**Design principle: Server has no LLM.** All semantic inference and SQL generation is done by the host agent. The server handles deterministic operations only — connecting, cataloging, indexing, querying.

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

Lucid stores business semantics as YAML files in `~/.lucid-mcp/semantic_store/`. These are:

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

## JOIN Path Discovery

`get_join_paths` automatically discovers how two tables can be joined — using foreign keys, matching column names, and embedding similarity. It finds both direct joins and indirect paths via one intermediate table, ranked by confidence.

## Business Domain Discovery

`get_business_domains` uses hierarchical clustering on table semantics and column overlap to group your tables into business domains (e.g., "Sales", "Inventory", "HR"). This gives the AI a high-level map of your data landscape before diving into specific queries.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LUCID_DATA_DIR` | `~/.lucid-mcp` | Data directory for catalog, semantic store, models |
| `LUCID_EMBEDDING_ENABLED` | `false` | Enable embedding hybrid search (downloads ~460MB model on first run) |

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
- [x] V1: Embedding-based hybrid search (BM25 + vector)
- [x] V2: JOIN path discovery, business domain clustering, get_overview
- [ ] V2: Parquet / large file support
- [ ] Commercial: Multi-tenancy, authentication, hosted version

---

## License

MIT
