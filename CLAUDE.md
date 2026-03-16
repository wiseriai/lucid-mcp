# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

lucid-skill is an AI-native data analysis skill that works as a CLI tool or MCP Server. It connects Claude and other AI apps to Excel, CSV, MySQL, and PostgreSQL data sources for schema discovery and SQL-based querying.

## Common Commands

```bash
npm run build         # Compile with tsup (output to dist/)
npm run dev           # Run directly with tsx (development)
npm test              # Run Vitest test suite
npm run lint          # ESLint on src/
npm run format        # Prettier on src/
```

Single test file: `npx vitest run tests/path/to/file.test.ts`

## Architecture

The server (`src/server.ts`) initializes an MCP server and registers tool handlers. Data flows through these layers:

1. **Tools** (`src/tools/`) — MCP tool handlers (connect_source, list_tables, query). Each tool validates params with Zod and returns MCP-formatted responses.

2. **Connectors** (`src/connectors/`) — Abstract `Connector` base class with implementations for Excel (DuckDB `read_xlsx`), CSV (DuckDB `read_csv_auto`), and MySQL (`mysql2/promise`). Each connector can register its data into the DuckDB query engine.

3. **Catalog** (`src/catalog/`) — SQLite-backed metadata store (`better-sqlite3`) with tables for sources, table metadata, and column metadata. The schema collector traverses connectors and persists metadata; the profiler uses DuckDB `SUMMARIZE`.

4. **Query Engine** (`src/query/`) — DuckDB-based execution with read-only safety validation (whitelist of SELECT/WITH only), automatic LIMIT injection, truncation detection, and result formatting (JSON/Markdown/CSV).

5. **Semantic Layer** (`src/semantic/`) — Sprint 2 stubs for YAML-based semantic definitions and BM25 FTS5 search index.

### Key design decisions

- All file-based sources (Excel, CSV) are loaded into DuckDB for unified SQL querying
- Safety checker uses a whitelist approach: only SELECT/WITH are allowed, all mutating statements are blocked
- Query results auto-apply a configurable `maxRows` limit (default 1000) with truncation detection
- Catalog persists across sessions via SQLite; DuckDB is in-memory per session

## Configuration

Defaults in `src/config.ts`: max 1000 rows, 30s timeout, 2GB memory limit, catalog at `./lucid-catalog.db`.

## Tech Stack

- **Runtime:** Node.js 20+, ESM modules
- **Build:** tsup (bundles to dist/index.js with shebang)
- **TypeScript:** strict mode, ES2022 target, bundler module resolution
- **Test:** Vitest (30s timeout)
- **Lint:** ESLint (warns on `any`, unused vars with `_` prefix ignored)
