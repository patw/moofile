/**
 * TypeScript definitions for MooFile.
 *
 * Documents are plain JSON-compatible objects; `_id` is always a string.
 */

/** Any value MooFile can store. */
export type Value =
    | string
    | number
    | boolean
    | null
    | Value[]
    | { [key: string]: Value };

/** A stored document. `_id` is present on anything read back from the store. */
export interface Document {
    _id?: string;
    [field: string]: Value | undefined;
}

/** A filter expression — field equality, or an operator object. */
export interface Filter {
    // The index signature must admit everything the named keys below can
    // hold, including a nested Filter for $not.
    [field: string]: Value | FilterOperators | Filter | Filter[] | undefined;
    $and?: Filter[];
    $or?: Filter[];
    $not?: Filter;
}

/** Comparison and membership operators usable in a filter. */
export interface FilterOperators {
    $eq?: Value;
    $ne?: Value;
    $gt?: Value;
    $gte?: Value;
    $lt?: Value;
    $lte?: Value;
    $in?: Value[];
    $nin?: Value[];
    $exists?: boolean;
    $elemMatch?: Filter;
}

/** Aggregation over a group. Every function except `count` needs a field. */
export interface Agg {
    func: 'count' | 'sum' | 'mean' | 'avg' | 'min' | 'max' | 'collect' | 'first' | 'last';
    field?: string;
}

/**
 * Query-builder options.
 *
 * Stages apply in the order: filter → group/agg → sort → skip → limit.
 * An unrecognised key is rejected rather than ignored.
 */
export interface FindOptions {
    /** Field to sort by. */
    sort?: string;
    /** Sort descending. Only meaningful alongside `sort`. */
    desc?: boolean;
    /** Skip the first N results. */
    skip?: number;
    /** Return at most N results. */
    limit?: number;
    /** Group by this field. */
    group?: string;
    /** Aggregations per group. A bare string is shorthand for `{ func }`. */
    agg?: (Agg | string)[];
}

/** Auto-embedding configuration for one source text field. */
export interface AutoEmbedConfig {
    /** GGUF model URI, e.g. "hf:user/repo:file.gguf". */
    model: string;
    /** Vector field the embedding is written to. */
    target: string;
    dims?: number;
    precision?: 'f32' | 'int8' | 'uint8' | 'binary';
    normalize?: boolean;
    query_prefix?: string;
    doc_prefix?: string;
}

/** Options for opening a collection. */
export interface CollectionConfig {
    /** Fields to build a B-tree index over. */
    indexes?: string[];
    /** Vector fields mapped to their dimensions. */
    vector_indexes?: Record<string, number>;
    /** Fields to build a BM25 text index over. */
    text_indexes?: string[];
    /** Source text fields to embed on insert, keyed by field name. */
    auto_embed?: Record<string, AutoEmbedConfig>;
    readonly?: boolean;
    /** Defaults to "os". */
    durability?: 'none' | 'os' | 'fsync';
    model_cache_dir?: string;
    /** Override shared-library discovery. */
    libPath?: string;
}

/** Collection statistics. */
export interface Stats {
    documents: number;
    /** A delete adds two (superseded original plus tombstone); an update, one. */
    dead_records: number;
    file_size_bytes: number;
    /** dead_records / total — the figure to threshold on before compact(). */
    dead_ratio: number;
}

/** A document paired with its similarity or relevance score. */
export interface SearchResult {
    doc: Document;
    score: number;
}

/** Raised for every MooFile failure. */
export class MooFileError extends Error {
    name: 'MooFileError';
}

/** Iterator over query results. Frees itself once exhausted. */
export class Cursor implements Iterable<Document> {
    /** Next document, or null when exhausted. */
    next(): Document | null;
    /** Collect all remaining documents. */
    toArray(): Document[];
    /** Release the cursor. Safe to call more than once. */
    close(): void;
    [Symbol.iterator](): Iterator<Document>;
}

/** Iterator over search results. Frees itself once exhausted. */
export class SearchCursor implements Iterable<SearchResult> {
    next(): SearchResult | null;
    toArray(): SearchResult[];
    close(): void;
    [Symbol.iterator](): Iterator<SearchResult>;
}

/** An open MooFile collection. */
export class Collection {
    constructor(path: string, config?: CollectionConfig);

    readonly path: string;

    /** Insert one document; returns it with `_id` populated. */
    insert(doc: Document): Document;

    /** Insert several documents; returns them with `_id`s populated. */
    insertMany(docs: Document[]): Document[];

    /** Find documents, optionally sorted, paged, grouped or aggregated. */
    find(filter?: Filter, options?: FindOptions | null): Cursor;

    /** First matching document, or null. */
    findOne(filter?: Filter): Document | null;

    /** Number of matching documents. */
    count(filter?: Filter): number;

    /** True if at least one document matches. */
    exists(filter: Filter): boolean;

    /**
     * Update the first matching document.
     *
     * @throws {MooFileError} if nothing matches — the same contract as the
     *   Rust and Python APIs. Call `exists()` first when a miss is expected.
     */
    updateOne(
        where: Filter,
        setValues?: Document,
        unsetFields?: string[],
        incValues?: Record<string, number>,
    ): boolean;

    /** Update all matching documents. Returns the count; 0 if none matched. */
    updateMany(
        where: Filter,
        setValues?: Document,
        unsetFields?: string[],
        incValues?: Record<string, number>,
    ): number;

    /**
     * Replace the first matching document, keeping its `_id`.
     *
     * @throws {MooFileError} if nothing matches.
     */
    replaceOne(where: Filter, replacement: Document): boolean;

    /** Delete the first matching document. False if nothing matched. */
    deleteOne(where: Filter): boolean;

    /** Delete all matching documents. Returns the count. */
    deleteMany(where: Filter): number;

    /** Cosine-similarity search over a vector field. */
    vectorSearch(
        field: string,
        queryVector: number[],
        limit?: number,
        filter?: Filter,
    ): SearchCursor;

    /** BM25 full-text search over a text field. */
    textSearch(
        field: string,
        query: string,
        limit?: number,
        filter?: Filter,
    ): SearchCursor;

    /**
     * Hybrid BM25 + vector search fused with Reciprocal Rank Fusion.
     * Pass `null` for `queryVector` to auto-embed `queryText`.
     */
    hybridSearch(
        textField: string,
        vectorField: string,
        queryText: string,
        queryVector?: number[] | null,
        limit?: number,
        filter?: Filter,
    ): SearchCursor;

    /**
     * Semantic search — auto-embeds `queryText` with the model configured for
     * `sourceField` via `auto_embed`.
     */
    semantic(
        sourceField: string,
        queryText: string,
        limit?: number,
        filter?: Filter,
    ): SearchCursor;

    batchBegin(): void;
    batchCommit(): void;
    batchRollback(): void;

    /** Run `fn` atomically. Rolls back if it throws, then rethrows. */
    batch<T>(fn: () => T): T;

    stats(): Stats;

    /** Rewrite the file, reclaiming space from dead records. */
    compact(): void;

    /** Flush and fsync. */
    sync(): void;

    /** Rebuild all in-memory indexes. */
    reindex(): void;

    /** Close the collection. Safe to call more than once. */
    close(): void;
}

/** Convenience wrapper for `new Collection(path, config)`. */
export function open(path: string, config?: CollectionConfig): Collection;
