import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createServer } from "./server.js";
import { runCli } from "./cli.js";

/**
 * Decide whether to run as CLI or MCP Server.
 * - No subcommand or "serve" → MCP Server (backward compatible)
 * - Any other subcommand → CLI dispatch
 * - --version / --help → CLI dispatch (no server needed)
 */
function shouldRunCli(): boolean {
  const arg = process.argv[2];
  if (!arg) return false; // no args → serve
  if (arg === "serve") return false;
  return true;
}

async function main(): Promise<void> {
  if (shouldRunCli()) {
    await runCli(process.argv);
    return;
  }

  const server = await createServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
  // Server is running via stdio — it will process messages until the transport closes.
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
