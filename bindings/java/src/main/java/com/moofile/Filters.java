package com.moofile;

import java.util.ArrayList;
import java.util.List;

/**
 * Static factories for MooFile's MongoDB-style filters.
 *
 * <p>Use static imports for concise, idiomatic Java queries:
 *
 * <pre>{@code
 * import static com.moofile.Filters.*;
 *
 * Document activeAdults = and(
 *     gte("age", 18),
 *     eq("status", "active"));
 *
 * List<Document> people = db.find(activeAdults);
 * }</pre>
 *
 * <p>These factories cover the filter operators MooFile supports. They return
 * ordinary {@link Document} instances, so they work with every existing API
 * that accepts a filter.
 */
public final class Filters {
    private Filters() { }

    /** Match a field exactly. */
    public static Document eq(String field, Object value) {
        return Document.of(requireField(field), value);
    }

    /** Match a field that does not equal a value. */
    public static Document ne(String field, Object value) {
        return fieldOperator(field, "$ne", value);
    }

    public static Document gt(String field, Object value)  { return fieldOperator(field, "$gt", value); }
    public static Document gte(String field, Object value) { return fieldOperator(field, "$gte", value); }
    public static Document lt(String field, Object value)  { return fieldOperator(field, "$lt", value); }
    public static Document lte(String field, Object value) { return fieldOperator(field, "$lte", value); }

    /** Match a field equal to any item in {@code values}. */
    public static Document in(String field, Iterable<?> values) {
        return fieldOperator(field, "$in", valuesToList(values));
    }

    /** Match a field unequal to every item in {@code values}. */
    public static Document nin(String field, Iterable<?> values) {
        return fieldOperator(field, "$nin", valuesToList(values));
    }

    /** Match documents where a field is present and non-null. */
    public static Document exists(String field) {
        return exists(field, true);
    }

    /** Match documents where a field is present/non-null when {@code exists} is true. */
    public static Document exists(String field, boolean exists) {
        return fieldOperator(field, "$exists", exists);
    }

    /** Match an array with at least one element satisfying {@code filter}. */
    public static Document elemMatch(String field, Document filter) {
        if (filter == null) throw new IllegalArgumentException("filter must not be null");
        return fieldOperator(field, "$elemMatch", filter);
    }

    /** Match documents satisfying every supplied filter. */
    public static Document and(Document... filters) {
        return logical("$and", filters);
    }

    /** Match documents satisfying at least one supplied filter. */
    public static Document or(Document... filters) {
        return logical("$or", filters);
    }

    /** Invert a filter. */
    public static Document not(Document filter) {
        if (filter == null) throw new IllegalArgumentException("filter must not be null");
        return Document.of("$not", filter);
    }

    private static Document fieldOperator(String field, String operator, Object value) {
        return Document.of(requireField(field), Document.of(operator, value));
    }

    private static Document logical(String operator, Document... filters) {
        if (filters == null) throw new IllegalArgumentException("filters must not be null");
        List<Object> documents = new ArrayList<>(filters.length);
        for (Document filter : filters) {
            if (filter == null) throw new IllegalArgumentException("filters must not contain null");
            documents.add(filter);
        }
        return Document.of(operator, documents);
    }

    private static List<Object> valuesToList(Iterable<?> values) {
        if (values == null) throw new IllegalArgumentException("values must not be null");
        List<Object> out = new ArrayList<>();
        for (Object value : values) out.add(value);
        return out;
    }

    private static String requireField(String field) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("field must not be null or empty");
        }
        return field;
    }
}
