/**
 * get_join_paths tool handler.
 * Discovers and returns JOIN paths between two tables.
 */

import type { CatalogStore } from "../catalog/store.js";
import type { Embedder } from "../semantic/embedder.js";
import { discoverJoinPaths, computeSchemaHash, discoverCrossSourceJoinPaths, computeCrossSourceSchemaHash, CROSS_SOURCE_ID } from "../discovery/joins.js";
import { discoverBusinessDomains } from "../discovery/domains.js";

/**
 * Handle get_join_paths request.
 * Checks cache, re-discovers if dirty, returns direct + indirect paths.
 */
export async function handleGetJoinPaths(
  params: Record<string, unknown>,
  catalog: CatalogStore,
  embedder?: Embedder | null,
): Promise<string> {
  const tableA = params.table_a as string;
  const tableB = params.table_b as string;

  if (!tableA || !tableB) {
    throw new Error("Both table_a and table_b are required");
  }

  // Find which source(s) contain these tables
  const allTables = catalog.getTables();
  const sourcesForA = new Set(allTables.filter((t) => t.table_name === tableA).map((t) => t.source_id));
  const sourcesForB = new Set(allTables.filter((t) => t.table_name === tableB).map((t) => t.source_id));

  // Validate both tables exist
  if (sourcesForA.size === 0) {
    throw new Error(`Table "${tableA}" not found in any connected source`);
  }
  if (sourcesForB.size === 0) {
    throw new Error(`Table "${tableB}" not found in any connected source`);
  }

  // Find common sources or all relevant sources
  const relevantSources = new Set<string>();
  for (const s of sourcesForA) {
    if (sourcesForB.has(s)) relevantSources.add(s);
  }
  // If no common source, include all sources (cross-source join)
  if (relevantSources.size === 0) {
    for (const s of sourcesForA) relevantSources.add(s);
    for (const s of sourcesForB) relevantSources.add(s);
  }

  // For each relevant source, check cache and discover if needed (within-source paths)
  for (const sourceId of relevantSources) {
    const meta = catalog.getCacheMeta(sourceId);
    const currentHash = computeSchemaHash(catalog, sourceId);

    const needsRefresh = !meta || meta.dirty === 1 || meta.schemaHash !== currentHash;

    if (needsRefresh) {
      catalog.clearJoinPaths(sourceId);
      const paths = await discoverJoinPaths(catalog, sourceId, embedder);
      for (const p of paths) {
        catalog.saveJoinPath(p);
      }
      catalog.setCacheMeta(sourceId, currentHash);
    }
  }

  // Cross-source discovery: check cache and discover if needed
  {
    const crossMeta = catalog.getCacheMeta(CROSS_SOURCE_ID);
    const crossHash = computeCrossSourceSchemaHash(catalog);
    const needsCrossRefresh = !crossMeta || crossMeta.dirty === 1 || crossMeta.schemaHash !== crossHash;

    if (needsCrossRefresh) {
      catalog.clearJoinPaths(CROSS_SOURCE_ID);
      const crossPaths = await discoverCrossSourceJoinPaths(catalog, embedder);
      for (const p of crossPaths) {
        catalog.saveJoinPath(p);
      }
      catalog.setCacheMeta(CROSS_SOURCE_ID, crossHash);
    }
  }

  // Fetch paths for the requested table pair (includes both within-source and cross-source)
  const paths = catalog.getJoinPaths(tableA, tableB);

  const directPaths = paths.filter((p) => !p.signalSource.startsWith("indirect:"));
  const indirectPaths = paths.filter((p) => p.signalSource.startsWith("indirect:"));

  const hasLowConfidence = paths.some((p) => p.confidence < 0.6);
  const warning = hasLowConfidence ? "low confidence, manual verification recommended" : null;

  return JSON.stringify({
    direct_paths: directPaths.map((p) => ({
      join_sql: p.sqlTemplate,
      confidence: Math.round(p.confidence * 100) / 100,
      signal: p.signalSource,
      join_condition: p.joinCondition,
    })),
    indirect_paths: indirectPaths.map((p) => ({
      via: p.signalSource.replace("indirect:via_", ""),
      join_sql: p.sqlTemplate,
      confidence: Math.round(p.confidence * 100) / 100,
      signal: p.signalSource,
      join_condition: p.joinCondition,
    })),
    warning,
  }, null, 2);
}

/**
 * Handle get_business_domains request.
 * Checks cache, re-discovers if dirty, returns clustered domains.
 */
export async function handleGetBusinessDomains(
  params: Record<string, unknown>,
  catalog: CatalogStore,
  embedder?: Embedder | null,
): Promise<string> {
  const datasource = params.datasource as string | undefined;

  // Determine which sources need refresh
  const sources = datasource
    ? catalog.getSources().filter((s) => s.id === datasource)
    : catalog.getSources();

  if (sources.length === 0 && datasource) {
    throw new Error(`Data source "${datasource}" not found`);
  }

  // Check if any source is dirty or has changed schema
  let needsRefresh = false;
  for (const source of sources) {
    const meta = catalog.getCacheMeta(source.id);
    const currentHash = computeSchemaHash(catalog, source.id);
    if (!meta || meta.dirty === 1 || meta.schemaHash !== currentHash) {
      needsRefresh = true;
      break;
    }
  }

  // Also refresh if no domains exist yet
  if (!needsRefresh && catalog.getDomains().length === 0) {
    needsRefresh = true;
  }

  if (needsRefresh) {
    catalog.clearDomains();
    const domains = await discoverBusinessDomains(catalog, embedder);
    for (const d of domains) {
      catalog.saveDomain(d);
    }
    // Update cache for all sources
    for (const source of sources) {
      const hash = computeSchemaHash(catalog, source.id);
      catalog.setCacheMeta(source.id, hash);
    }
  }

  const domains = catalog.getDomains();

  // Filter by datasource if specified
  let filteredDomains = domains;
  if (datasource) {
    const sourceTables = new Set(
      catalog.getTables(datasource).map((t) => t.table_name),
    );
    filteredDomains = domains
      .map((d) => ({
        ...d,
        tableNames: d.tableNames.filter((t) => sourceTables.has(t)),
      }))
      .filter((d) => d.tableNames.length > 0);
  }

  // Compute unclustered tables
  const allTables = datasource
    ? catalog.getTables(datasource)
    : catalog.getTables();
  const clusteredTables = new Set(filteredDomains.flatMap((d) => d.tableNames));
  const unclustered = allTables
    .map((t) => t.table_name)
    .filter((t) => !clusteredTables.has(t));

  return JSON.stringify({
    domains: filteredDomains.map((d) => ({
      name: d.domainName,
      tables: d.tableNames,
      keywords: d.keywords,
      table_count: d.tableNames.length,
    })),
    total_tables: allTables.length,
    unclustered,
  }, null, 2);
}
