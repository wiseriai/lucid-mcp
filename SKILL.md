---
name: lucid-skill
description: "Connect and query business data (Excel, CSV, MySQL, PostgreSQL) with natural language. Use when the user wants to analyze data files, query databases, understand table relationships, explore business domains, or ask questions like 'which product had the highest sales?', 'how do orders and customers relate?'. Install via npm, then use the lucid-skill CLI."
metadata:
  {
    "openclaw":
      {
        "emoji": "📊",
        "requires": { "bins": ["lucid-skill"] },
        "install":
          [
            {
              "id": "npm",
              "kind": "npm",
              "package": "@wiseria/lucid-skill",
              "bins": ["lucid-skill"],
              "label": "Install lucid-skill (npm)",
            },
          ],
      },
  }
---

# lucid-skill

AI-native data analysis. Connect Excel/CSV/MySQL/PostgreSQL, infer business semantics, query with SQL.

All output is JSON (except `query` which defaults to markdown). No API key needed.

## Install

```bash
npm install -g @wiseria/lucid-skill
```

Verify:

```bash
lucid-skill --version
lucid-skill overview     # Empty sources on first run is normal
```

## CLI Commands

| Command | Purpose |
|---------|---------|
| `overview` | **Start here.** Shows all connected sources, tables, semantic status. |
| `connect csv <path>` | Connect a CSV file. |
| `connect excel <path>` | Connect an Excel file. Use `--sheets Sheet1,Sheet2` to select sheets. |
| `connect mysql --host h --database db --username u --password p` | Connect MySQL. |
| `connect postgres --host h --database db --username u --password p [--schema s]` | Connect PostgreSQL. |
| `tables` | List all connected tables with row counts. |
| `describe <table>` | Column details + sample data + semantics. |
| `profile <table>` | Deep stats: null rate, distinct, min/max, quartiles. |
| `init-semantic` | Export schemas for semantic inference. |
| `update-semantic <file\|->` | Save semantic definitions (JSON from file or stdin). |
| `search <query>` | Natural language → relevant tables + JOIN hints + metric SQL. |
| `join-paths <a> <b>` | Discover JOIN paths between two tables. |
| `domains` | Auto-discovered business domains. |
| `query <sql>` | Execute read-only SQL. Default: markdown. `--format json\|csv` for other formats. |
| `serve` | Start MCP Server (stdio JSON-RPC). |

## Workflow: First Time

```bash
lucid-skill overview                                    # 1. Check current state
lucid-skill connect csv /path/to/data.csv               # 2. Connect data
lucid-skill init-semantic                               # 3. Get schema for inference
# Analyze output, infer business meanings, then:
echo '{"tables":[...]}' | lucid-skill update-semantic - # 4. Save semantics
lucid-skill search "用户的问题"                          # 5. Find relevant tables
lucid-skill join-paths orders customers                 # 6. Discover JOINs
lucid-skill query "SELECT ..."                          # 7. Execute and return
```

## Workflow: Returning (auto-restores previous connections)

```bash
lucid-skill overview                     # See what's already connected
lucid-skill search "用户的问题"           # Find relevant tables
lucid-skill query "SELECT ..."           # Execute
```

## Smart Query Pattern

When a user asks a data question:

1. `lucid-skill search "关键词"` — find relevant tables
2. Check `suggestedJoins` and `suggestedMetricSqls` in the response
3. If multi-table: `lucid-skill join-paths table_a table_b` — get correct JOIN SQL
4. Compose SQL from the returned context
5. `lucid-skill query "SELECT ..."` — execute and present results

## update-semantic JSON Format

```json
{
  "tables": [{
    "table_name": "orders",
    "description": "订单主表",
    "business_domain": "电商/交易",
    "tags": ["核心表", "财务"],
    "columns": [
      { "name": "amount", "semantic": "订单金额", "role": "measure", "unit": "CNY", "aggregation": "sum" },
      { "name": "created_at", "semantic": "下单时间", "role": "timestamp" }
    ],
    "relations": [
      { "target_table": "customers", "join_condition": "orders.customer_id = customers.id", "relation_type": "many_to_one" }
    ],
    "metrics": [
      { "name": "日GMV", "expression": "SUM(amount)", "group_by": "DATE(created_at)" }
    ]
  }]
}
```

Column roles: `measure`, `dimension`, `timestamp`, `id`, `attribute`.

## Key Facts

- **Read-only**: Only SELECT allowed. INSERT/UPDATE/DELETE/DROP blocked.
- **Auto-restore**: Previous connections survive restarts. Always check `overview` first.
- **Semantic layer**: YAML files in `~/.lucid-mcp/semantic_store/`, human-readable.
- **Data directory**: `~/.lucid-mcp/` (override with `LUCID_DATA_DIR` env var).
- **Embedding**: Optional. Set `LUCID_EMBEDDING_ENABLED=true` for better multilingual search (downloads ~460 MB model on first use).
- **No credentials stored**: Database passwords are never written to disk.
