package com.moofile;

/**
 * MooFile — lightweight embedded document store (Java binding).
 *
 * Calls libmoofile via JNR-FFI or Panama FFI.  This implementation uses
 * the JNR-FFI approach which works with JDK 8+.
 *
 * Build:
 *   javac -cp jnr-ffi.jar:jnr-ffi-2.2.15.jar src/main/java/com/moofile/*.java
 *
 * Usage:
 *   import com.moofile.*;
 *
 *   Collection db = Collection.open("data.bson",
 *       Config.create().index("email").vectorIndex("emb", 384));
 *
 *   Document doc = db.insert(Document.parse("{\"name\":\"Alice\"}"));
 *   System.out.println(doc);
 *
 *   db.close();
 */

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * A JSON-serializable document — wraps a Map&lt;String, Object&gt;.
 */
class Document {
    final Map<String, Object> data;

    public Document() { this.data = new LinkedHashMap<>(); }
    public Document(Map<String, Object> data) { this.data = data; }

    public Document put(String key, Object value) {
        data.put(key, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Object get(String key) { return data.get(key); }

    public String toJson() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var e : data.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append(jsonEscape(e.getKey())).append(":");
            sb.append(jsonValue(e.getValue()));
        }
        sb.append("}");
        return sb.toString();
    }

    public static Document parse(String json) {
        return new Document(parseJson(json));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJson(String json) {
        // Minimal JSON parser — in production use Jackson/Gson
        Map<String, Object> map = new LinkedHashMap<>();
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) return map;
        json = json.substring(1, json.length() - 1).trim();

        int depth = 0;
        int i = 0;
        while (i < json.length()) {
            // Skip to key
            while (i < json.length() && Character.isWhitespace(json.charAt(i))) i++;
            if (i >= json.length()) break;

            // Parse key
            if (json.charAt(i) != '"') break;
            int keyEnd = json.indexOf('"', i + 1);
            if (keyEnd < 0) break;
            String key = json.substring(i + 1, keyEnd);
            i = keyEnd + 1;

            // Skip colon
            while (i < json.length() && (json.charAt(i) == ':' || Character.isWhitespace(json.charAt(i)))) i++;

            // Parse value
            if (i < json.length() && json.charAt(i) == '{') {
                int end = findMatching(json, i);
                map.put(key, parseJson(json.substring(i, end + 1)));
                i = end + 1;
            } else if (i < json.length() && json.charAt(i) == '[') {
                int end = findMatching(json, i);
                map.put(key, parseJsonArray(json.substring(i, end + 1)));
                i = end + 1;
            } else if (i < json.length() && json.charAt(i) == '"') {
                int valEnd = json.indexOf('"', i + 1);
                if (valEnd < 0) break;
                map.put(key, json.substring(i + 1, valEnd));
                i = valEnd + 1;
            } else {
                // number, boolean, null
                int valEnd = i;
                while (valEnd < json.length() && json.charAt(valEnd) != ',' && json.charAt(valEnd) != '}') valEnd++;
                String val = json.substring(i, valEnd).trim();
                if (val.equals("null")) map.put(key, null);
                else if (val.equals("true")) map.put(key, true);
                else if (val.equals("false")) map.put(key, false);
                else if (val.contains(".")) map.put(key, Double.parseDouble(val));
                else map.put(key, Long.parseLong(val));
                i = valEnd;
            }

            // Skip comma
            while (i < json.length() && (json.charAt(i) == ',' || Character.isWhitespace(json.charAt(i)))) i++;
        }
        return map;
    }

    private static List<Object> parseJsonArray(String json) {
        List<Object> list = new ArrayList<>();
        json = json.trim();
        if (!json.startsWith("[") || !json.endsWith("]")) return list;
        json = json.substring(1, json.length() - 1).trim();
        // Simplified: split by top-level commas
        int depth = 0;
        int start = 0;
        for (int i = 0; i <= json.length(); i++) {
            if (i == json.length() || (json.charAt(i) == ',' && depth == 0)) {
                String item = json.substring(start, i).trim();
                if (!item.isEmpty()) {
                    if (item.startsWith("\"")) list.add(item.substring(1, item.length() - 1));
                    else if (item.equals("null")) list.add(null);
                    else if (item.equals("true")) list.add(true);
                    else if (item.equals("false")) list.add(false);
                    else if (item.contains(".")) list.add(Double.parseDouble(item));
                    else list.add(Long.parseLong(item));
                }
                start = i + 1;
            } else if (json.charAt(i) == '{' || json.charAt(i) == '[') depth++;
            else if (json.charAt(i) == '}' || json.charAt(i) == ']') depth--;
        }
        return list;
    }

    private static int findMatching(String s, int start) {
        char open = s.charAt(start);
        char close = open == '{' ? '}' : ']';
        int depth = 0;
        for (int i = start; i < s.length(); i++) {
            if (s.charAt(i) == open) depth++;
            else if (s.charAt(i) == close) { depth--; if (depth == 0) return i; }
        }
        return s.length() - 1;
    }

    private String jsonValue(Object v) {
        if (v == null) return "null";
        if (v instanceof String) return jsonEscape((String) v);
        if (v instanceof Boolean || v instanceof Number) return v.toString();
        if (v instanceof Map) return new Document((Map<String,Object>)v).toJson();
        if (v instanceof List) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object e : (List)v) {
                if (!first) sb.append(",");
                first = false;
                sb.append(jsonValue(e));
            }
            return sb.append("]").toString();
        }
        return jsonEscape(v.toString());
    }

    private String jsonEscape(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
    }

    @Override
    public String toString() { return toJson(); }
}
