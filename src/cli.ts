/**
 * CLI entry point for lucid-mcp.
 * Parses process.argv and dispatches to the appropriate tool handler.
 */

import { readFileSync } from "node:fs";
import { CatalogStore } from "./catalog/store.js";
import { QueryEngine } from "./query/engine.js";
import { QueryRouter } from "./query/router.js";
import { SemanticIndex } from "./semantic/index.js";
import { Embedder } from "./semantic/embedder.js";
import { autoRestoreConnections } from "./startup.js";
import { getConfig } from "./config.js";
import { handleGetOverview } from "./tools/overview.js";
import { handleConnectSource, handleListTables } from "./tools/connect.js";
import { handleDescribeTable } from "./tools/describe.js";
import { handleProfileData } from "./tools/profile.js";
import { handleQuery } from "./tools/query.js";
import { handleInitSemantic, handleUpdateSemantic } from "./tools/semantic.js";
import { handleSearchTables } from "./tools/search.js";
import { handleGetJoinPaths, handleGetBusinessDomains } from "./tools/discovery.js";

// ── Runtime singleton ────────────────────────────────────────────────────────

interface Runtime {
  catalog: CatalogStore;
  engine: QueryEngine;
  router: QueryRouter;
  semanticIndex: SemanticIndex;
  embedder: Embedder | null;
}

async function initRuntime(): Promise<Runtime> {
  const config = getConfig();
  const catalog = new CatalogStore();
  const engine = new QueryEngine();
  const router = new QueryRouter(engine);
  const semanticIndex = new SemanticIndex();

  let embedder: Embedder | null = null;
  if (config.embedding.enabled) {
    embedder = Embedder.getInstance();
    embedder.init().catch(() => {});
  }

  const { restored, failed } = await autoRestoreConnections(catalog, engine, router, semanticIndex, embedder);
  if (restored > 0 || failed.length > 0) {
    process.stderr.write(
      `[lucid-mcp] restored ${restored} source(s)${failed.length ? `, failed: ${failed.join("; ")}` : ""}\n`,
    );
  }

  return { catalog, engine, router, semanticIndex, embedder };
}

// ── Argument parsing helpers ─────────────────────────────────────────────────

function getFlag(args: string[], name: string): string | undefined {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1 || idx + 1 >= args.length) return undefined;
  return args[idx + 1];
}

function hasFlag(args: string[], name: string): boolean {
  return args.includes(`--${name}`);
}

// ── Read stdin fully ─────────────────────────────────────────────────────────

function readStdin(): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    process.stdin.on("data", (chunk) => chunks.push(chunk));
    process.stdin.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    process.stdin.on("error", reject);
  });
}

// ── Version from package.json ────────────────────────────────────────────────

function getVersion(): string {
  try {
    // Works both from src/ (dev) and dist/ (production)
    const pkgPath = new URL("../package.json", import.meta.url);
    const pkg = JSON.parse(readFileSync(pkgPath, "utf-8"));
    return pkg.version ?? "unknown";
  } catch {
    return "unknown";
  }
}

// ── Help text ────────────────────────────────────────────────────────────────

const HELP = `lucid-mcp — AI-native data analysis agent

Usage: lucid-mcp <command> [args] [--flags]

Commands:
  serve                              Start MCP Server (default)
  overview                           Show connected sources overview
  connect csv <path>                 Connect a CSV file or directory
  connect excel <path> [--sheets s]  Connect an Excel file
  connect mysql --host h --database db --username u --password p [--port 3306]
  connect postgres --host h --database db --username u --password p [--port 5432] [--schema s]
  tables                             List all connected tables
  describe <table>                   Describe a table's structure
  profile <table>                    Profile a table's data
  init-semantic                      Export schemas for semantic inference
  update-semantic <file|->           Write semantic definitions (JSON)
  search <query> [--top-k 5]         Search semantic layer
  join-paths <tableA> <tableB>       Discover JOIN paths
  domains                            Discover business domains
  query <sql> [--format json|md|csv] Execute read-only SQL

Flags:
  --version                          Print version
  --help                             Print this help
`;

// ── Subcommand dispatch ──────────────────────────────────────────────────────

export async function runCli(argv: string[]): Promise<void> {
  // argv[0]=node, argv[1]=script, argv[2..]=args
  const args = argv.slice(2);

  // Global flags
  if (args.includes("--version") || args.includes("-v")) {
    process.stdout.write(getVersion() + "\n");
    return;
  }
  if (args.includes("--help") || args.includes("-h")) {
    process.stdout.write(HELP);
    return;
  }

  const cmd = args[0];
  if (!cmd || cmd === "serve") {
    // Handled by index.ts — should not reach here
    return;
  }

  // All other commands need runtime
  const rt = await initRuntime();

  try {
    switch (cmd) {
      case "overview": {
        const result = handleGetOverview(rt.catalog, rt.semanticIndex);
        process.stdout.write(result + "\n");
        break;
      }

      case "connect": {
        const type = args[1];
        if (!type) throw new Error("Usage: lucid-mcp connect <csv|excel|mysql|postgres>");

        let params: Record<string, unknown>;
        switch (type) {
          case "csv": {
            const p = args[2];
            if (!p) throw new Error("Usage: lucid-mcp connect csv <path>");
            params = { type: "csv", path: p };
            break;
          }
          case "excel": {
            const p = args[2];
            if (!p) throw new Error("Usage: lucid-mcp connect excel <path> [--sheets s1,s2]");
            params = { type: "excel", path: p } as Record<string, unknown>;
            const sheets = getFlag(args, "sheets");
            if (sheets) (params as Record<string, unknown>).sheets = sheets.split(",");
            break;
          }
          case "mysql": {
            params = {
              type: "mysql",
              host: getFlag(args, "host"),
              port: getFlag(args, "port") ? Number(getFlag(args, "port")) : undefined,
              database: getFlag(args, "database"),
              username: getFlag(args, "username"),
              password: getFlag(args, "password"),
            };
            if (!params.host || !params.database || !params.username)
              throw new Error("Usage: lucid-mcp connect mysql --host h --database db --username u --password p");
            break;
          }
          case "postgres": {
            params = {
              type: "postgresql",
              host: getFlag(args, "host"),
              port: getFlag(args, "port") ? Number(getFlag(args, "port")) : undefined,
              database: getFlag(args, "database"),
              username: getFlag(args, "username"),
              password: getFlag(args, "password"),
              schema: getFlag(args, "schema"),
            };
            if (!params.host || !params.database || !params.username)
              throw new Error("Usage: lucid-mcp connect postgres --host h --database db --username u --password p");
            break;
          }
          default:
            throw new Error(`Unknown source type: ${type}. Use csv, excel, mysql, or postgres.`);
        }

        const result = await handleConnectSource(params, rt.catalog, rt.engine, rt.router);
        process.stdout.write(JSON.stringify({
          success: true,
          sourceId: result.sourceId,
          tables: result.tables.map((t) => ({
            name: t.name,
            rowCount: t.rowCount,
            columnCount: t.columns.length,
          })),
        }, (_k, v) => typeof v === "bigint" ? String(v) : v, 2) + "\n");
        break;
      }

      case "tables": {
        const sourceId = getFlag(args, "source-id");
        const tables = await handleListTables(sourceId ? { sourceId } : {}, rt.catalog);
        process.stdout.write(JSON.stringify(tables, null, 2) + "\n");
        break;
      }

      case "describe": {
        const table = args[1];
        if (!table) throw new Error("Usage: lucid-mcp describe <table_name>");
        const result = await handleDescribeTable({ table_name: table }, rt.catalog, rt.engine);
        process.stdout.write(result + "\n");
        break;
      }

      case "profile": {
        const table = args[1];
        if (!table) throw new Error("Usage: lucid-mcp profile <table_name>");
        const result = await handleProfileData({ table_name: table }, rt.catalog, rt.engine);
        process.stdout.write(result + "\n");
        break;
      }

      case "init-semantic": {
        const sourceId = getFlag(args, "source-id");
        const result = await handleInitSemantic(sourceId ? { source_id: sourceId } : {}, rt.catalog, rt.engine);
        process.stdout.write(result + "\n");
        break;
      }

      case "update-semantic": {
        const fileArg = args[1];
        if (!fileArg) throw new Error("Usage: lucid-mcp update-semantic <json_file | ->");

        let jsonStr: string;
        if (fileArg === "-") {
          jsonStr = await readStdin();
        } else {
          jsonStr = readFileSync(fileArg, "utf-8");
        }

        const data = JSON.parse(jsonStr);
        const result = handleUpdateSemantic(data, rt.catalog, rt.semanticIndex, rt.embedder);
        process.stdout.write(result + "\n");
        break;
      }

      case "search": {
        const query = args[1];
        if (!query) throw new Error("Usage: lucid-mcp search <query> [--top-k 5]");
        const topK = getFlag(args, "top-k") ? Number(getFlag(args, "top-k")) : 5;
        const result = await handleSearchTables({ query, top_k: topK }, rt.semanticIndex, rt.catalog, rt.embedder);
        process.stdout.write(result + "\n");
        break;
      }

      case "join-paths": {
        const tableA = args[1];
        const tableB = args[2];
        if (!tableA || !tableB) throw new Error("Usage: lucid-mcp join-paths <table_a> <table_b>");
        const result = await handleGetJoinPaths({ table_a: tableA, table_b: tableB }, rt.catalog, rt.embedder);
        process.stdout.write(result + "\n");
        break;
      }

      case "domains": {
        const datasource = getFlag(args, "datasource");
        const result = await handleGetBusinessDomains(datasource ? { datasource } : {}, rt.catalog, rt.embedder);
        process.stdout.write(result + "\n");
        break;
      }

      case "query": {
        const sql = args[1];
        if (!sql) throw new Error("Usage: lucid-mcp query <sql> [--format json|markdown|csv]");
        const format = getFlag(args, "format") ?? "markdown";
        const result = await handleQuery({ sql, format }, rt.engine, rt.router);
        process.stdout.write(result + "\n");
        break;
      }

      default:
        process.stderr.write(`Unknown command: ${cmd}\nRun 'lucid-mcp --help' for usage.\n`);
        process.exit(1);
    }
  } catch (err) {
    process.stderr.write(`Error: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exit(1);
  }
}
