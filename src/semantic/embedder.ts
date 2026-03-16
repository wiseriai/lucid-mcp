/**
 * Singleton Embedder class using @xenova/transformers for multilingual embeddings.
 * State machine: idle → initializing → ready | error
 */

import { getConfig } from "../config.js";

type EmbedderState = "idle" | "initializing" | "ready" | "error";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Pipeline = any;

export class Embedder {
  private static instance: Embedder | null = null;

  private state: EmbedderState = "idle";
  private pipeline: Pipeline = null;
  private initPromise: Promise<void> | null = null;
  private modelName: string;
  private cacheDir: string;

  private constructor() {
    const config = getConfig();
    this.modelName = config.embedding.model;
    this.cacheDir = config.embedding.cacheDir;
  }

  static getInstance(): Embedder {
    if (!Embedder.instance) {
      Embedder.instance = new Embedder();
    }
    return Embedder.instance;
  }

  /** Reset singleton (for testing) */
  static resetInstance(): void {
    Embedder.instance = null;
  }

  /** Idempotent async initialization — multiple calls return the same promise. */
  async init(): Promise<void> {
    if (this.state === "ready") return;
    if (this.initPromise) return this.initPromise;

    this.state = "initializing";
    this.initPromise = this.doInit();
    return this.initPromise;
  }

  private async doInit(): Promise<void> {
    try {
      // Dynamic import to avoid loading the large module when embedding is disabled
      const { pipeline } = await import("@xenova/transformers");
      this.pipeline = await pipeline("feature-extraction", this.modelName, {
        cache_dir: this.cacheDir,
      });
      this.state = "ready";
      process.stderr.write(`[lucid-skill] embedder: model ${this.modelName} loaded\n`);
    } catch (err) {
      this.state = "error";
      this.initPromise = null;
      process.stderr.write(
        `[lucid-skill] embedder: failed to load model — ${err instanceof Error ? err.message : String(err)}\n`,
      );
    }
  }

  isReady(): boolean {
    return this.state === "ready";
  }

  getModelId(): string {
    return this.modelName;
  }

  /** Generate embedding for text. Throws if not ready. */
  async embed(text: string): Promise<Float32Array> {
    if (this.state !== "ready" || !this.pipeline) {
      throw new Error("Embedder not ready. Call init() first and wait for it to complete.");
    }

    const output = await this.pipeline(text, { pooling: "mean", normalize: true });
    // output.data is a Float32Array after mean pooling + normalize
    return new Float32Array(output.data);
  }

  /** Cosine similarity between two vectors. */
  static cosineSimilarity(a: Float32Array, b: Float32Array): number {
    if (a.length !== b.length) return 0;
    let dot = 0;
    let normA = 0;
    let normB = 0;
    for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    const denom = Math.sqrt(normA) * Math.sqrt(normB);
    return denom === 0 ? 0 : dot / denom;
  }
}
