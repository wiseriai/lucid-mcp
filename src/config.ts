import type { LucidConfig } from "./types.js";
import os from "node:os";
import path from "node:path";

/**
 * Resolve the data directory for lucid-skill.
 * Priority:
 * 1. LUCID_DATA_DIR env var (explicit override)
 * 2. ~/.lucid-skill/ (user home, always writable, works regardless of cwd)
 */
function resolveDataDir(): string {
  if (process.env.LUCID_DATA_DIR) {
    return process.env.LUCID_DATA_DIR;
  }
  return path.join(os.homedir(), ".lucid-skill");
}

const dataDir = resolveDataDir();

const DEFAULT_CONFIG: LucidConfig = {
  server: {
    name: "lucid-skill",
    version: "1.0.0",
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
  embedding: {
    enabled: process.env.LUCID_EMBEDDING_ENABLED === "true",
    model: "Xenova/paraphrase-multilingual-MiniLM-L12-v2",
    cacheDir: path.join(os.homedir(), ".lucid-skill", "models"),
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
    embedding: { ...currentConfig.embedding, ...partial.embedding },
    logging: { ...currentConfig.logging, ...partial.logging },
  };
}
