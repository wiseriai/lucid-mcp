/**
 * Semantic layer — YAML read/write for table semantics.
 * Stores table-level business meaning as YAML files under semantic_store/{source_id}/{table}.yaml
 */

import fs from "node:fs";
import path from "node:path";
import yaml from "js-yaml";
import type { TableSemantic } from "../types.js";
import { getConfig } from "../config.js";

/**
 * Sanitize a source ID for use as a directory name.
 * "mysql:mydb" → "mysql_mydb", "csv:orders.csv" → "csv_orders_csv"
 */
function sanitizeSourceId(sourceId: string): string {
  return sourceId.replace(/[^a-zA-Z0-9_\u4e00-\u9fff-]/g, "_");
}

/**
 * Get the file path for a table's semantic YAML.
 */
function getSemanticFilePath(sourceId: string, tableName: string): string {
  const config = getConfig();
  const dir = path.join(config.semantic.storePath, sanitizeSourceId(sourceId));
  return path.join(dir, `${tableName}.yaml`);
}

/**
 * Read a table's semantic definition from YAML.
 * Returns null if the file doesn't exist.
 */
export function readTableSemantic(
  sourceId: string,
  tableName: string,
): TableSemantic | null {
  const filePath = getSemanticFilePath(sourceId, tableName);
  if (!fs.existsSync(filePath)) {
    return null;
  }
  try {
    const content = fs.readFileSync(filePath, "utf-8");
    return yaml.load(content) as TableSemantic;
  } catch {
    return null;
  }
}

/**
 * Write a table's semantic definition to YAML.
 * Creates directory structure if needed.
 */
export function writeTableSemantic(
  sourceId: string,
  tableName: string,
  semantic: TableSemantic,
): void {
  const filePath = getSemanticFilePath(sourceId, tableName);
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });

  // Ensure updatedAt is set
  semantic.updatedAt = new Date().toISOString();

  const content = yaml.dump(semantic, {
    indent: 2,
    lineWidth: 120,
    noRefs: true,
    sortKeys: false,
  });

  fs.writeFileSync(filePath, content, "utf-8");
}

/**
 * List all semantic definitions for a source.
 */
export function listSemantics(sourceId: string): TableSemantic[] {
  const config = getConfig();
  const dir = path.join(config.semantic.storePath, sanitizeSourceId(sourceId));

  if (!fs.existsSync(dir)) {
    return [];
  }

  const files = fs.readdirSync(dir).filter((f) => f.endsWith(".yaml"));
  const results: TableSemantic[] = [];

  for (const file of files) {
    try {
      const content = fs.readFileSync(path.join(dir, file), "utf-8");
      const semantic = yaml.load(content) as TableSemantic;
      if (semantic) results.push(semantic);
    } catch {
      // Skip invalid files
    }
  }

  return results;
}

/**
 * List all semantic definitions across all sources.
 */
export function listAllSemantics(): TableSemantic[] {
  const config = getConfig();
  const storePath = config.semantic.storePath;

  if (!fs.existsSync(storePath)) {
    return [];
  }

  const results: TableSemantic[] = [];
  const sourceDirs = fs
    .readdirSync(storePath, { withFileTypes: true })
    .filter((d) => d.isDirectory());

  for (const dir of sourceDirs) {
    const fullDir = path.join(storePath, dir.name);
    const files = fs.readdirSync(fullDir).filter((f) => f.endsWith(".yaml"));
    for (const file of files) {
      try {
        const content = fs.readFileSync(path.join(fullDir, file), "utf-8");
        const semantic = yaml.load(content) as TableSemantic;
        if (semantic) results.push(semantic);
      } catch {
        // Skip invalid files
      }
    }
  }

  return results;
}

/**
 * Delete a table's semantic definition.
 */
export function deleteTableSemantic(
  sourceId: string,
  tableName: string,
): boolean {
  const filePath = getSemanticFilePath(sourceId, tableName);
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
    return true;
  }
  return false;
}
