package com.moofile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small, complete JSON reader/writer.
 *
 * <p>MooFile passes every document across the FFI boundary as JSON, so this
 * needs to be correct rather than merely convenient. It is a proper
 * recursive-descent parser: it handles string escapes (including surrogate
 * pairs via {@code \\uXXXX}), exponent notation, nested arrays and objects,
 * and commas inside strings. Splitting JSON on commas — the obvious shortcut —
 * corrupts any document containing a vector, a nested object, or a comma
 * inside a string value.
 *
 * <p>The mapping is: {@code null} → null, booleans → {@link Boolean}, integers
 * → {@link Long}, reals → {@link Double}, strings → {@link String}, arrays →
 * {@link List}, objects → {@link LinkedHashMap} (insertion-ordered, so a
 * round trip preserves field order).
 *
 * <p>This class is deliberately dependency-free. Swap it for Jackson or Gson
 * if you already have one on the classpath.
 */
public final class Json {

    private Json() {}

    /** Thrown when input is not well-formed JSON. */
    public static class SyntaxException extends RuntimeException {
        public SyntaxException(String msg) { super(msg); }
    }

    // -----------------------------------------------------------------
    // Parsing
    // -----------------------------------------------------------------

    /** Parse any JSON value. */
    public static Object parse(String json) {
        Parser p = new Parser(json);
        p.skipWhitespace();
        Object value = p.parseValue();
        p.skipWhitespace();
        if (!p.atEnd()) {
            throw new SyntaxException("trailing content at offset " + p.pos);
        }
        return value;
    }

    /** Parse a JSON object into a map. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String json) {
        Object value = parse(json);
        if (!(value instanceof Map)) {
            throw new SyntaxException("expected a JSON object, got "
                + (value == null ? "null" : value.getClass().getSimpleName()));
        }
        return (Map<String, Object>) value;
    }

    /** Parse a JSON array into a list. */
    @SuppressWarnings("unchecked")
    public static List<Object> parseArray(String json) {
        Object value = parse(json);
        if (!(value instanceof List)) {
            throw new SyntaxException("expected a JSON array, got "
                + (value == null ? "null" : value.getClass().getSimpleName()));
        }
        return (List<Object>) value;
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) { this.s = s; }

        boolean atEnd() { return pos >= s.length(); }

        void skipWhitespace() {
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++;
                else break;
            }
        }

        private char peek() {
            if (pos >= s.length()) throw new SyntaxException("unexpected end of input");
            return s.charAt(pos);
        }

        private void expect(char c) {
            if (pos >= s.length() || s.charAt(pos) != c) {
                throw new SyntaxException("expected '" + c + "' at offset " + pos);
            }
            pos++;
        }

        Object parseValue() {
            skipWhitespace();
            char c = peek();
            switch (c) {
                case '{': return parseObject();
                case '[': return parseArray();
                case '"': return parseString();
                case 't': return parseLiteral("true", Boolean.TRUE);
                case 'f': return parseLiteral("false", Boolean.FALSE);
                case 'n': return parseLiteral("null", null);
                default:  return parseNumber();
            }
        }

        private Object parseLiteral(String word, Object value) {
            if (!s.startsWith(word, pos)) {
                throw new SyntaxException("invalid literal at offset " + pos);
            }
            pos += word.length();
            return value;
        }

        Map<String, Object> parseObject() {
            expect('{');
            Map<String, Object> map = new LinkedHashMap<>();
            skipWhitespace();
            if (peek() == '}') { pos++; return map; }

            while (true) {
                skipWhitespace();
                String key = parseString();
                skipWhitespace();
                expect(':');
                map.put(key, parseValue());
                skipWhitespace();
                char c = peek();
                if (c == ',') { pos++; continue; }
                if (c == '}') { pos++; return map; }
                throw new SyntaxException("expected ',' or '}' at offset " + pos);
            }
        }

        List<Object> parseArray() {
            expect('[');
            List<Object> list = new ArrayList<>();
            skipWhitespace();
            if (peek() == ']') { pos++; return list; }

            while (true) {
                list.add(parseValue());
                skipWhitespace();
                char c = peek();
                if (c == ',') { pos++; continue; }
                if (c == ']') { pos++; return list; }
                throw new SyntaxException("expected ',' or ']' at offset " + pos);
            }
        }

        String parseString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (pos >= s.length()) throw new SyntaxException("unterminated string");
                char c = s.charAt(pos++);
                if (c == '"') return sb.toString();
                if (c != '\\') { sb.append(c); continue; }

                if (pos >= s.length()) throw new SyntaxException("unterminated escape");
                char esc = s.charAt(pos++);
                switch (esc) {
                    case '"':  sb.append('"');  break;
                    case '\\': sb.append('\\'); break;
                    case '/':  sb.append('/');  break;
                    case 'b':  sb.append('\b'); break;
                    case 'f':  sb.append('\f'); break;
                    case 'n':  sb.append('\n'); break;
                    case 'r':  sb.append('\r'); break;
                    case 't':  sb.append('\t'); break;
                    case 'u':
                        if (pos + 4 > s.length()) {
                            throw new SyntaxException("truncated \\u escape");
                        }
                        sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                        pos += 4;
                        break;
                    default:
                        throw new SyntaxException("invalid escape '\\" + esc + "'");
                }
            }
        }

        Object parseNumber() {
            int start = pos;
            if (pos < s.length() && (s.charAt(pos) == '-' || s.charAt(pos) == '+')) pos++;
            boolean isReal = false;
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c >= '0' && c <= '9') { pos++; }
                else if (c == '.' || c == 'e' || c == 'E') { isReal = true; pos++; }
                else if ((c == '-' || c == '+')
                         && (s.charAt(pos - 1) == 'e' || s.charAt(pos - 1) == 'E')) { pos++; }
                else break;
            }
            if (start == pos) throw new SyntaxException("expected a value at offset " + pos);

            String text = s.substring(start, pos);
            if (isReal) return Double.parseDouble(text);
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException e) {
                // Out of long range — keep it as a double rather than failing.
                return Double.parseDouble(text);
            }
        }
    }

    // -----------------------------------------------------------------
    // Writing
    // -----------------------------------------------------------------

    /** Serialise any supported value to JSON. */
    public static String write(Object value) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, value);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String str) {
            writeString(sb, str);
        } else if (v instanceof Boolean || v instanceof Integer || v instanceof Long
                   || v instanceof Short || v instanceof Byte) {
            sb.append(v);
        } else if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                // JSON has no way to spell these; null is the conventional stand-in.
                sb.append("null");
            } else {
                sb.append(v);
            }
        } else if (v instanceof Number) {
            sb.append(v);
        } else if (v instanceof Document doc) {
            // Documents nest inside filters and updates
            // (e.g. {"age": {"$gt": 30}}); without this they would fall
            // through to toString() and be written as a JSON *string*.
            writeValue(sb, doc.asMap());
        } else if (v instanceof Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                writeValue(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object e : it) {
                if (!first) sb.append(',');
                first = false;
                writeValue(sb, e);
            }
            sb.append(']');
        } else if (v.getClass().isArray()) {
            sb.append('[');
            int n = java.lang.reflect.Array.getLength(v);
            for (int i = 0; i < n; i++) {
                if (i > 0) sb.append(',');
                writeValue(sb, java.lang.reflect.Array.get(v, i));
            }
            sb.append(']');
        } else {
            writeString(sb, v.toString());
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b");  break;
                case '\f': sb.append("\\f");  break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        sb.append('"');
    }
}
