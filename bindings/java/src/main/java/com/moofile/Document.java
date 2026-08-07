package com.moofile;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A MooFile document — an insertion-ordered map of field names to values.
 *
 * <p>Values are the plain Java types {@link Json} produces: {@link String},
 * {@link Long}, {@link Double}, {@link Boolean}, {@link List}, nested
 * {@link Map}, and null.
 *
 * <pre>{@code
 * Document doc = new Document()
 *     .put("name", "Alice")
 *     .put("age", 30)
 *     .put("tags", List.of("admin", "ops"));
 * }</pre>
 */
public class Document {

    private final Map<String, Object> data;

    public Document() {
        this.data = new LinkedHashMap<>();
    }

    public Document(Map<String, Object> data) {
        this.data = data == null ? new LinkedHashMap<>() : new LinkedHashMap<>(data);
    }

    /** Build a document from alternating key/value arguments. */
    public static Document of(Object... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("expected alternating key/value arguments");
        }
        Document d = new Document();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            d.put(String.valueOf(keyValuePairs[i]), keyValuePairs[i + 1]);
        }
        return d;
    }

    /** Set a field. Returns this, for chaining. */
    public Document put(String key, Object value) {
        data.put(key, value);
        return this;
    }

    public Object get(String key) {
        return data.get(key);
    }

    /** Get a field as a string, or null if absent. */
    public String getString(String key) {
        Object v = data.get(key);
        return v == null ? null : v.toString();
    }

    /**
     * Get a numeric field as a long.
     *
     * @throws MooFileException if the field is absent or not a number
     */
    public long getLong(String key) {
        Object v = data.get(key);
        if (v instanceof Number n) return n.longValue();
        throw new MooFileException("field '" + key + "' is not a number: " + v);
    }

    /**
     * Get a numeric field as a double.
     *
     * @throws MooFileException if the field is absent or not a number
     */
    public double getDouble(String key) {
        Object v = data.get(key);
        if (v instanceof Number n) return n.doubleValue();
        throw new MooFileException("field '" + key + "' is not a number: " + v);
    }

    /** Get a boolean field, defaulting to false when absent. */
    public boolean getBoolean(String key) {
        return Boolean.TRUE.equals(data.get(key));
    }

    /** Get a list field, or null if absent. */
    @SuppressWarnings("unchecked")
    public List<Object> getList(String key) {
        Object v = data.get(key);
        return v instanceof List ? (List<Object>) v : null;
    }

    /** Get a nested object field as a Document, or null if absent. */
    @SuppressWarnings("unchecked")
    public Document getDocument(String key) {
        Object v = data.get(key);
        return v instanceof Map ? new Document((Map<String, Object>) v) : null;
    }

    /** The document's {@code _id}, or null before insertion. */
    public String id() {
        return getString("_id");
    }

    public boolean containsKey(String key) { return data.containsKey(key); }
    public Object remove(String key)       { return data.remove(key); }
    public int size()                      { return data.size(); }
    public boolean isEmpty()               { return data.isEmpty(); }
    public java.util.Set<String> keySet()  { return data.keySet(); }

    /** The backing map. Mutating it mutates the document. */
    public Map<String, Object> asMap() { return data; }

    public String toJson() { return Json.write(data); }

    /** Parse a JSON object into a document. */
    public static Document parse(String json) {
        return new Document(Json.parseObject(json));
    }

    @Override
    public String toString() { return toJson(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof Document other && data.equals(other.data);
    }

    @Override
    public int hashCode() { return data.hashCode(); }
}
