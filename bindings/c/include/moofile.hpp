#ifndef MOOFILE_HPP
#define MOOFILE_HPP

/**
 * C++ wrapper for the MooFile C API.
 *
 * Provides RAII resource management and idiomatic C++ ergonomics while
 * preserving the full MongoDB-style API.
 *
 * Dependencies: a C++17 compiler, a JSON library (nlohmann/json recommended).
 *
 * Example:
 * @code
 * #include "moofile.hpp"
 * #include <nlohmann/json.hpp>
 * using json = nlohmann::json;
 *
 * int main() {
 *     // Basic usage
 *     moofile::Collection db("data.bson",
 *         moofile::Config{}
 *             .index("email")
 *             .vector_index("embedding", 384)
 *             .text_index("content")
 *     );
 *
 *     db.insert({{"name", "Alice"}, {"email", "a@example.com"}});
 *
 *     for (auto doc : db.find({{"age", {{"$gt", 25}}}}).to_vector()) {
 *         std::cout << doc.dump() << std::endl;
 *     }
 *
 *     auto results = db.vector_search("embedding", {0.1, 0.2, 0.3}, 5).to_vector();
 *     for (auto [doc, score] : results) {
 *         std::cout << score << ": " << doc.dump() << std::endl;
 *     }
 *
 *     // Autoembedding (local GGUF model, no external API)
 *     moofile::Config auto_cfg;
 *     auto_cfg.vector_index("embedding", 1024)
 *             .auto_embed("content", {
 *                 .model = "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
 *                 .target = "embedding",
 *                 .dims = 1024,
 *                 .precision = "int8",
 *             });
 *     moofile::Collection auto_db("semantic.bson", auto_cfg);
 *     auto_db.insert({{"content", "Machine learning is fascinating"}});
 *     // embedding auto-generated in doc["embedding"]
 *
 *     auto sem_results = auto_db.semantic("content", "deep learning", 5).to_vector();
 *     for (auto [doc, score] : sem_results) {
 *         std::cout << score << ": " << doc.dump() << std::endl;
 *     }
 * }
 * @endcode
 */

#include "moofile.h"

#include <nlohmann/json.hpp>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace moofile {

// ---------------------------------------------------------------------------
// Exception
// ---------------------------------------------------------------------------

/** Base exception class for all MooFile C++ errors. */
class error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// ---------------------------------------------------------------------------
// Utility: JSON helpers
// ---------------------------------------------------------------------------

using json = nlohmann::json;

inline json parse_json(const char* s) {
    if (!s) return nullptr;
    auto result = json::parse(s, nullptr, false);
    if (result.is_discarded()) {
        throw error("failed to parse JSON");
    }
    return result;
}

// ---------------------------------------------------------------------------
// Config builder
// ---------------------------------------------------------------------------

/** Auto-embedding configuration for a single source text field. */
struct AutoEmbedConfig {
    std::string model;           // GGUF model URI (e.g. "hf:user/repo:file.gguf")
    std::string target;          // Target vector field name
    int dims = 1024;             // Embedding dimensions
    std::string precision = "int8";  // "f32", "int8", "uint8", "binary"
    bool normalize = true;       // L2-normalize the output
    std::string query_prefix = "Represent the query for retrieving supporting documents: ";
    std::string doc_prefix = "Represent the document for retrieval: ";
};

/** Configuration builder for opening a collection. */
struct Config {
    std::vector<std::string> indexes;
    std::vector<std::pair<std::string, int>> vector_indexes;
    std::vector<std::string> text_indexes;
    std::vector<std::pair<std::string, AutoEmbedConfig>> auto_embeds;
    bool readonly = false;
    std::string durability = "os";
    std::string model_cache_dir;

    Config& index(const std::string& field) {
        indexes.push_back(field);
        return *this;
    }

    Config& vector_index(const std::string& field, int dims) {
        vector_indexes.emplace_back(field, dims);
        return *this;
    }

    Config& text_index(const std::string& field) {
        text_indexes.push_back(field);
        return *this;
    }

    Config& auto_embed(const std::string& source_field, const AutoEmbedConfig& cfg) {
        auto_embeds.emplace_back(source_field, cfg);
        return *this;
    }

    Config& set_readonly(bool r = true) {
        readonly = r;
        return *this;
    }

    Config& set_durability(const std::string& d) {
        durability = d;
        return *this;
    }

    Config& set_model_cache_dir(const std::string& d) {
        model_cache_dir = d;
        return *this;
    }

    std::string to_json() const {
        json j;
        if (!indexes.empty()) j["indexes"] = indexes;
        if (!vector_indexes.empty()) {
            json vi = json::object();
            for (auto& [f, d] : vector_indexes) vi[f] = d;
            j["vector_indexes"] = vi;
        }
        if (!text_indexes.empty()) j["text_indexes"] = text_indexes;
        if (!auto_embeds.empty()) {
            json ae = json::object();
            for (auto& [field, cfg] : auto_embeds) {
                ae[field] = {
                    {"model", cfg.model},
                    {"target", cfg.target},
                    {"dims", cfg.dims},
                    {"precision", cfg.precision},
                    {"normalize", cfg.normalize},
                    {"query_prefix", cfg.query_prefix},
                    {"doc_prefix", cfg.doc_prefix},
                };
            }
            j["auto_embed"] = ae;
        }
        if (readonly) j["readonly"] = true;
        j["durability"] = durability;
        if (!model_cache_dir.empty()) j["model_cache_dir"] = model_cache_dir;
        return j.dump();
    }
};

/**
 * Serialise a filter/document argument for the C layer.
 *
 * Brace-initialising an nlohmann `json` from a bare `{}` yields *null*, not an
 * empty object, so the natural-looking `db.count({})` would otherwise be
 * rejected by the C layer with "expected JSON object at top level".  Null and
 * an empty array (the other thing `{}` can decay to) both mean "no
 * constraints", so normalise them to an empty object.
 */
inline std::string dump_doc(const json& j) {
    if (j.is_null()) return "{}";
    if (j.is_array() && j.empty()) return "{}";
    return j.dump();
}

// ---------------------------------------------------------------------------
// Find options (sort / skip / limit / group / agg)
// ---------------------------------------------------------------------------

/**
 * Query-builder options for Collection::find().
 *
 * Chainable, mirroring the Rust and Python query chains:
 *
 *     db.find({{"active", true}},
 *             moofile::FindOptions().sort("age", true).limit(10));
 *
 * Stages apply in the order: filter → group/agg → sort → skip → limit.
 */
class FindOptions {
public:
    /** Sort by a field.  `desc` selects descending order. */
    FindOptions& sort(const std::string& field, bool desc = false) {
        sort_field_ = field;
        sort_desc_ = desc;
        return *this;
    }

    /** Skip the first `n` results. */
    FindOptions& skip(int64_t n) { skip_ = n; return *this; }

    /** Return at most `n` results. */
    FindOptions& limit(int64_t n) { limit_ = n; return *this; }

    /** Group by a field.  Combine with agg() to aggregate each group. */
    FindOptions& group(const std::string& field) { group_ = field; return *this; }

    /** Aggregate the number of documents per group. */
    FindOptions& count() { return agg("count", ""); }

    /** Aggregate a field per group: "sum", "mean", "min", "max",
     *  "collect", "first", "last". */
    FindOptions& agg(const std::string& func, const std::string& field) {
        aggs_.emplace_back(func, field);
        return *this;
    }

    FindOptions& sum(const std::string& f)     { return agg("sum", f); }
    FindOptions& mean(const std::string& f)    { return agg("mean", f); }
    FindOptions& min(const std::string& f)     { return agg("min", f); }
    FindOptions& max(const std::string& f)     { return agg("max", f); }
    FindOptions& collect(const std::string& f) { return agg("collect", f); }
    FindOptions& first(const std::string& f)   { return agg("first", f); }
    FindOptions& last(const std::string& f)    { return agg("last", f); }

    std::string to_json() const {
        json j = json::object();
        if (!sort_field_.empty()) {
            j["sort"] = json{{"field", sort_field_}, {"desc", sort_desc_}};
        }
        if (skip_ > 0) j["skip"] = skip_;
        if (limit_ >= 0) j["limit"] = limit_;
        if (!group_.empty()) j["group"] = group_;
        if (!aggs_.empty()) {
            json arr = json::array();
            for (const auto& [func, field] : aggs_) {
                json entry = json{{"func", func}};
                if (!field.empty()) entry["field"] = field;
                arr.push_back(entry);
            }
            j["agg"] = arr;
        }
        return j.dump();
    }

    /** True when nothing has been set — lets find() skip the extra call. */
    bool empty() const {
        return sort_field_.empty() && skip_ == 0 && limit_ < 0
            && group_.empty() && aggs_.empty();
    }

private:
    std::string sort_field_;
    bool sort_desc_ = false;
    int64_t skip_ = 0;
    int64_t limit_ = -1;
    std::string group_;
    std::vector<std::pair<std::string, std::string>> aggs_;
};

// ---------------------------------------------------------------------------
// Result cursor
// ---------------------------------------------------------------------------

/** RAII cursor for iterating over query results. */
class Cursor {
public:
    Cursor(MooFileCursor* c) : cursor_(c) {
        if (!c) throw error("null cursor");
    }

    ~Cursor() { moofile_cursor_free(cursor_); }

    Cursor(Cursor&& other) noexcept : cursor_(other.cursor_) {
        other.cursor_ = nullptr;
    }

    Cursor& operator=(Cursor&& other) noexcept {
        if (this != &other) {
            moofile_cursor_free(cursor_);
            cursor_ = other.cursor_;
            other.cursor_ = nullptr;
        }
        return *this;
    }

    Cursor(const Cursor&) = delete;
    Cursor& operator=(const Cursor&) = delete;

    /** Get the next document, or nullopt when exhausted. */
    std::optional<json> next() {
        char* err = nullptr;
        char* s = moofile_cursor_next(cursor_, &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        if (!s) return std::nullopt;
        json doc = parse_json(s);
        moofile_free_string(s);
        return doc;
    }

    /** Collect all remaining documents into a vector. */
    std::vector<json> to_vector() {
        std::vector<json> result;
        while (auto doc = next()) {
            result.push_back(std::move(*doc));
        }
        return result;
    }

private:
    MooFileCursor* cursor_;
};

// ---------------------------------------------------------------------------
// Search result cursor
// ---------------------------------------------------------------------------

/** RAII cursor for iterating over search results (doc, score) pairs. */
class SearchCursor {
public:
    SearchCursor(MooFileSearchCursor* c) : cursor_(c) {
        if (!c) throw error("null search cursor");
    }

    ~SearchCursor() { moofile_search_cursor_free(cursor_); }

    SearchCursor(SearchCursor&& other) noexcept : cursor_(other.cursor_) {
        other.cursor_ = nullptr;
    }

    SearchCursor& operator=(SearchCursor&& other) noexcept {
        if (this != &other) {
            moofile_search_cursor_free(cursor_);
            cursor_ = other.cursor_;
            other.cursor_ = nullptr;
        }
        return *this;
    }

    SearchCursor(const SearchCursor&) = delete;
    SearchCursor& operator=(const SearchCursor&) = delete;

    /** Get the next (doc, score) pair, or nullopt when exhausted. */
    std::optional<std::pair<json, float>> next() {
        char* err = nullptr;
        char* s = moofile_search_cursor_next(cursor_, &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        if (!s) return std::nullopt;
        json pair = parse_json(s);
        moofile_free_string(s);
        if (!pair.is_array() || pair.size() < 2) {
            throw error("malformed search result");
        }
        return std::make_pair(pair[0], pair[1].get<float>());
    }

    /** Collect all remaining results into a vector of (doc, score) pairs. */
    std::vector<std::pair<json, float>> to_vector() {
        std::vector<std::pair<json, float>> result;
        while (auto item = next()) {
            result.push_back(std::move(*item));
        }
        return result;
    }

private:
    MooFileSearchCursor* cursor_;
};

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/**
 * RAII wrapper for a MooFile collection.
 *
 * Opens the collection on construction, closes on destruction.
 * All CRUD operations are exposed as methods returning JSON types.
 */
class Collection {
public:
    /**
     * Open a collection.
     *
     * @param path   Path to the .bson file (created if absent).
     * @param config Configuration (indexes, vector_indexes, etc.).
     */
    Collection(const std::string& path, const Config& config = Config{})
        : path_(path)
    {
        auto config_json = config.to_json();
        char* err = nullptr;
        handle_ = moofile_open(path.c_str(), config_json.c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        if (!handle_) {
            throw error("failed to open collection: " + path);
        }
    }

    ~Collection() {
        if (handle_) {
            char* err = nullptr;
            moofile_close(handle_, &err);
            moofile_free_string(err);
        }
    }

    Collection(Collection&& other) noexcept
        : handle_(other.handle_), path_(std::move(other.path_))
    {
        other.handle_ = nullptr;
    }

    Collection& operator=(Collection&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                char* err = nullptr;
                moofile_close(handle_, &err);
                moofile_free_string(err);
            }
            handle_ = other.handle_;
            path_ = std::move(other.path_);
            other.handle_ = nullptr;
        }
        return *this;
    }

    Collection(const Collection&) = delete;
    Collection& operator=(const Collection&) = delete;

    // ----------------------------------------------------------
    // Insert
    // ----------------------------------------------------------

    /** Insert a single document. Returns the inserted doc (with _id). */
    json insert(const json& doc) {
        return json::parse(exec([&](char** err) {
            return moofile_insert(handle_, dump_doc(doc).c_str(), err);
        }));
    }

    /** Insert multiple documents. Returns the inserted docs (with _ids). */
    json insert_many(const json& docs) {
        return json::parse(exec([&](char** err) {
            return moofile_insert_many(handle_, dump_doc(docs).c_str(), err);
        }));
    }

    // ----------------------------------------------------------
    // Query
    // ----------------------------------------------------------

    /** Find documents matching a filter. Returns a Cursor. */
    Cursor find(const json& filter = json::object()) {
        char* err = nullptr;
        auto* c = moofile_find(handle_, dump_doc(filter).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return Cursor(c);
    }

    /**
     * Find with sort / skip / limit / group / agg.
     *
     *     auto docs = db.find({{"active", true}},
     *                         FindOptions().sort("age", true).limit(10))
     *                   .to_vector();
     */
    Cursor find(const json& filter, const FindOptions& options) {
        auto options_json = options.to_json();
        char* err = nullptr;
        auto* c = moofile_find_ex(handle_, dump_doc(filter).c_str(),
                                  options_json.c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return Cursor(c);
    }

    /** Find the first matching document, or nullopt. */
    std::optional<json> find_one(const json& filter = json::object()) {
        char* err = nullptr;
        char* s = moofile_find_one(handle_, dump_doc(filter).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        if (!s) return std::nullopt;
        json doc = json::parse(s, nullptr, false);
        moofile_free_string(s);
        if (doc.is_discarded()) throw error("failed to parse find_one result");
        return doc;
    }

    /** Count documents matching a filter. */
    int64_t count(const json& filter = json::object()) {
        char* err = nullptr;
        int64_t n = moofile_count(handle_, dump_doc(filter).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return n;
    }

    /** Check if at least one document matches a filter. */
    bool exists(const json& filter) {
        char* err = nullptr;
        int r = moofile_exists(handle_, dump_doc(filter).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return r == 1;
    }

    // ----------------------------------------------------------
    // Update
    // ----------------------------------------------------------

    /**
     * Update the first matching document.
     *
     * @param where  Filter to match the document.
     * @param set    Fields to set (or empty json::object()).
     * @param unset  Fields to remove (or empty vector).
     * @param inc    Fields to increment (or empty json::object()).
     * @return true if a document was updated.
     */
    bool update_one(
        const json& where,
        const json& set_values = json::object(),
        const std::vector<std::string>& unset_fields = {},
        const json& inc_values = json::object()
    ) {
        json update = json::object();
        if (!set_values.empty()) update["set"] = set_values;
        if (!unset_fields.empty()) update["unset"] = unset_fields;
        if (!inc_values.empty()) update["inc"] = inc_values;

        char* err = nullptr;
        int r = moofile_update_one(handle_, dump_doc(where).c_str(),
                                    dump_doc(update).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return r == 1;
    }

    /**
     * Update all matching documents.
     * @return The number of documents updated.
     */
    int64_t update_many(
        const json& where,
        const json& set_values = json::object(),
        const std::vector<std::string>& unset_fields = {},
        const json& inc_values = json::object()
    ) {
        json update = json::object();
        if (!set_values.empty()) update["set"] = set_values;
        if (!unset_fields.empty()) update["unset"] = unset_fields;
        if (!inc_values.empty()) update["inc"] = inc_values;

        char* err = nullptr;
        int64_t n = moofile_update_many(handle_, dump_doc(where).c_str(),
                                         dump_doc(update).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return n;
    }

    /**
     * Replace the first matching document entirely.
     * @return true if a document was replaced.
     */
    bool replace_one(const json& where, const json& replacement) {
        char* err = nullptr;
        int r = moofile_replace_one(handle_, dump_doc(where).c_str(),
                                     dump_doc(replacement).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return r == 1;
    }

    // ----------------------------------------------------------
    // Delete
    // ----------------------------------------------------------

    /** Delete the first matching document. Returns true if deleted. */
    bool delete_one(const json& where) {
        char* err = nullptr;
        int r = moofile_delete_one(handle_, dump_doc(where).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return r == 1;
    }

    /** Delete all matching documents. Returns the count deleted. */
    int64_t delete_many(const json& where) {
        char* err = nullptr;
        int64_t n = moofile_delete_many(handle_, dump_doc(where).c_str(), &err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return n;
    }

    // ----------------------------------------------------------
    // Vector search
    // ----------------------------------------------------------

    /** Vector similarity search. Returns (doc, score) pairs. */
    SearchCursor vector_search(
        const std::string& field,
        const std::vector<float>& query_vector,
        int limit = 10,
        const json& filter = json::object()
    ) {
        json vec_json = query_vector;
        char* err = nullptr;
        auto* c = moofile_vector_search(
            handle_, dump_doc(filter).c_str(),
            field.c_str(), vec_json.dump().c_str(),
            limit, &err
        );
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return SearchCursor(c);
    }

    // ----------------------------------------------------------
    // Text search
    // ----------------------------------------------------------

    /** BM25 full-text search. Returns (doc, score) pairs. */
    SearchCursor text_search(
        const std::string& field,
        const std::string& query,
        int limit = 10,
        const json& filter = json::object()
    ) {
        char* err = nullptr;
        auto* c = moofile_text_search(
            handle_, dump_doc(filter).c_str(),
            field.c_str(), query.c_str(),
            limit, &err
        );
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return SearchCursor(c);
    }

    // ----------------------------------------------------------
    // Hybrid search (RRF)
    // ----------------------------------------------------------

    /**
     * Hybrid search combining BM25 + vector similarity via RRF.
     *
     * @param text_field    The BM25 text field.
     * @param vector_field  The vector field.
     * @param query_text    The text query.
     * @param query_vector  The vector query (or empty to auto-embed).
     * @param limit         Max results.
     * @param filter        Optional pre-filter.
     */
    SearchCursor hybrid_search(
        const std::string& text_field,
        const std::string& vector_field,
        const std::string& query_text,
        const std::vector<float>& query_vector = {},
        int limit = 10,
        const json& filter = json::object()
    ) {
        auto vec_str = query_vector.empty()
            ? std::string()
            : json(query_vector).dump();
        char* err = nullptr;
        auto* c = moofile_hybrid_search(
            handle_, dump_doc(filter).c_str(),
            text_field.c_str(), vector_field.c_str(),
            query_text.c_str(),
            vec_str.empty() ? nullptr : vec_str.c_str(),
            limit, &err
        );
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return SearchCursor(c);
    }

    // ----------------------------------------------------------
    // Semantic search (autoembedding)
    // ----------------------------------------------------------

    /**
     * Semantic search — auto-embeds the query text.
     *
     * The `source_field` must have been configured with `auto_embed` at
     * collection open time.  The query text is automatically prefixed
     * with the configured `query_prefix` and embedded.
     *
     * @param source_field The text field configured in auto_embed.
     * @param query_text   The search query (auto-embedded).
     * @param limit        Max results.
     * @param filter       Optional pre-filter.
     */
    SearchCursor semantic(
        const std::string& source_field,
        const std::string& query_text,
        int limit = 10,
        const json& filter = json::object()
    ) {
        char* err = nullptr;
        auto* c = moofile_semantic_search(
            handle_, dump_doc(filter).c_str(),
            source_field.c_str(), query_text.c_str(),
            limit, &err
        );
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        return SearchCursor(c);
    }

    // ----------------------------------------------------------
    // Batch writes
    // ----------------------------------------------------------

    void batch_begin() {
        char* err = nullptr;
        if (moofile_batch_begin(handle_, &err) != 0) {
            std::string msg(err ? err : "batch_begin failed");
            moofile_free_string(err);
            throw error(msg);
        }
    }

    void batch_commit() {
        char* err = nullptr;
        if (moofile_batch_commit(handle_, &err) != 0) {
            std::string msg(err ? err : "batch_commit failed");
            moofile_free_string(err);
            throw error(msg);
        }
    }

    void batch_rollback() {
        char* err = nullptr;
        moofile_batch_rollback(handle_, &err);
        moofile_free_string(err);
    }

    /** RAII helper for batch writes. */
    class Batch {
    public:
        Batch(Collection& db) : db_(db) { db_.batch_begin(); }
        ~Batch() {
            if (committed_) db_.batch_commit();
            else db_.batch_rollback();
        }
        void commit() { committed_ = true; }
    private:
        Collection& db_;
        bool committed_ = false;
    };

    // ----------------------------------------------------------
    // Utility
    // ----------------------------------------------------------

    /** Get collection statistics. */
    json stats() {
        return json::parse(exec([&](char** err) {
            return moofile_stats(handle_, err);
        }));
    }

    /** Compact the data file. */
    void compact() {
        char* err = nullptr;
        if (moofile_compact(handle_, &err) != 0) {
            std::string msg(err ? err : "compact failed");
            moofile_free_string(err);
            throw error(msg);
        }
    }

    /** Flush and fsync the data file. */
    void sync() {
        char* err = nullptr;
        if (moofile_sync(handle_, &err) != 0) {
            std::string msg(err ? err : "sync failed");
            moofile_free_string(err);
            throw error(msg);
        }
    }

    /** Rebuild all indexes from scratch. */
    void reindex() {
        char* err = nullptr;
        if (moofile_reindex(handle_, &err) != 0) {
            std::string msg(err ? err : "reindex failed");
            moofile_free_string(err);
            throw error(msg);
        }
    }

private:
    MooFileCollection* handle_ = nullptr;
    std::string path_;

    /**
     * Helper: call a C function that returns an owned char*, translating an
     * `err_out` message into an exception and freeing the result.
     *
     * `f` receives the `char**` to pass along as the call's err_out argument.
     */
    template<typename F>
    std::string exec(F&& f) const {
        char* err = nullptr;
        char* s = f(&err);
        if (err) {
            std::string msg(err);
            moofile_free_string(err);
            throw error(msg);
        }
        if (!s) throw error("null result");
        std::string result(s);
        moofile_free_string(s);
        return result;
    }
};

} // namespace moofile

#endif // MOOFILE_HPP
