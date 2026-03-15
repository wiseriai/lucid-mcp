import type { LucidConfig } from "./types.js";
import os from "node:os";
import path from "node:path";

/**
 * Resolve the data directory for Lucid MCP.
 * Priority:
 * 1. LUCID_DATA_DIR env var (explicit override)
 * 2. ~/.lucid-mcp/ (user home, always writable, works regardless of cwd)
 */
function resolveDataDir(): string {
  if (process.env.LUCID_DATA_DIR) {
    return process.env.LUCID_DATA_DIR;
  }
  return path.join(os.homedir(), ".lucid-mcp");
}

const dataDir = resolveDataDir();

const DEFAULT_CONFIG: LucidConfig = {
  server: {
    name: "lucid-mcp",
    version: "0.1.0",
    transport: "stdio",
  },
  query: {
    maxRows: 1000,
    timeoutSeconds: 30,
    memoryLimit: "2GB",
  },
  semantic: {
    storePath: path.join(dataDir, "semantic_store"),
  },
  catalog: {
    dbPath: path.join(dataDir, "lucid-catalog.db"),
    autoProfile: true,
  },
  logging: {
    level: "info",
  },
};

let currentConfig: LucidConfig = { ...DEFAULT_CONFIG };

export function getConfig(): LucidConfig {
  return currentConfig;
}

export function updateConfig(partial: Partial<LucidConfig>): void {
  currentConfig = {
    ...currentConfig,
    ...partial,
    server: { ...currentConfig.server, ...partial.server },
    query: { ...currentConfig.query, ...partial.query },
    semantic: { ...currentConfig.semantic, ...partial.semantic },
    catalog: { ...currentConfig.catalog, ...partial.catalog },
    logging: { ...currentConfig.logging, ...partial.logging },
  };
}
