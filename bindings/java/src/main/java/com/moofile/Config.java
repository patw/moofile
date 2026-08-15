package com.moofile;

import java.util.*;

/**
 * Configuration for opening a MooFile collection.
 *
 * Usage:
 *   Config cfg = Config.create()
 *       .index("email")
 *       .vectorIndex("embedding", 384)
 *       .textIndex("content")
 *       .autoEmbed("content", AutoEmbedConfig.of("BAAI/bge-small-en-v1.5", "embedding"));
 */
public class Config {
    final List<String> indexes = new ArrayList<>();
    final Map<String, Integer> vectorIndexes = new LinkedHashMap<>();
    final List<String> textIndexes = new ArrayList<>();
    final Map<String, AutoEmbedConfig> autoEmbeds = new LinkedHashMap<>();
    boolean readonly = false;
    String durability = "os";
    String modelCacheDir = "";

    private Config() {}

    public static Config create() { return new Config(); }

    public Config index(String field) { indexes.add(field); return this; }
    public Config vectorIndex(String field, int dims) { vectorIndexes.put(field, dims); return this; }
    public Config textIndex(String field) { textIndexes.add(field); return this; }
    public Config autoEmbed(String sourceField, AutoEmbedConfig cfg) { autoEmbeds.put(sourceField, cfg); return this; }
    public Config readonly(boolean r) { this.readonly = r; return this; }
    public Config durability(String d) { this.durability = d; return this; }
    public Config modelCacheDir(String d) { this.modelCacheDir = d; return this; }

    /** Serialize to JSON for the C API. */
    public String toJson() {
        Map<String, Object> cfg = new LinkedHashMap<>();
        if (!indexes.isEmpty()) cfg.put("indexes", indexes);
        if (!vectorIndexes.isEmpty()) cfg.put("vector_indexes", vectorIndexes);
        if (!textIndexes.isEmpty()) cfg.put("text_indexes", textIndexes);
        if (!autoEmbeds.isEmpty()) {
            Map<String, Object> ae = new LinkedHashMap<>();
            for (var e : autoEmbeds.entrySet()) {
                ae.put(e.getKey(), e.getValue().toMap());
            }
            cfg.put("auto_embed", ae);
        }
        if (readonly) cfg.put("readonly", true);
        cfg.put("durability", durability);
        if (!modelCacheDir.isEmpty()) cfg.put("model_cache_dir", modelCacheDir);
        return new Document(cfg).toJson();
    }

    /** Auto-embedding configuration for a single source text field. */
    public static class AutoEmbedConfig {
        public String model, target;
        public int dims = 384;
        public String precision = "int8";
        public boolean normalize = true;
        public String queryPrefix = "Represent the query for retrieving supporting documents: ";
        public String docPrefix = "Represent the document for retrieval: ";

        private AutoEmbedConfig() {}

        public static AutoEmbedConfig of(String model, String target) {
            AutoEmbedConfig c = new AutoEmbedConfig();
            c.model = model; c.target = target;
            return c;
        }

        public AutoEmbedConfig dims(int d) { this.dims = d; return this; }
        public AutoEmbedConfig precision(String p) { this.precision = p; return this; }
        public AutoEmbedConfig normalize(boolean n) { this.normalize = n; return this; }
        public AutoEmbedConfig queryPrefix(String p) { this.queryPrefix = p; return this; }
        public AutoEmbedConfig docPrefix(String p) { this.docPrefix = p; return this; }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("model", model); m.put("target", target); m.put("dims", dims);
            m.put("precision", precision); m.put("normalize", normalize);
            m.put("query_prefix", queryPrefix); m.put("doc_prefix", docPrefix);
            return m;
        }
    }
}
