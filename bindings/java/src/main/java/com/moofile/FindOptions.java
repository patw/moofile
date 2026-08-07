package com.moofile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Query-builder options for {@link Collection#find(Document, FindOptions)}.
 *
 * <p>Stages apply in the order: filter → group/agg → sort → skip → limit.
 *
 * <pre>{@code
 * // Top 10 oldest active users
 * db.find(Document.of("active", true),
 *         FindOptions.create().sort("age", true).limit(10));
 *
 * // Headcount and payroll per department
 * db.find(null, FindOptions.create()
 *     .group("dept").count().sum("pay").sort("dept"));
 * }</pre>
 */
public class FindOptions {

    private String sortField;
    private boolean sortDesc;
    private int skip;
    private int limit = -1;
    private String groupField;
    private final List<Map<String, Object>> aggs = new ArrayList<>();

    private FindOptions() {}

    public static FindOptions create() { return new FindOptions(); }

    /** Sort ascending by a field. */
    public FindOptions sort(String field) { return sort(field, false); }

    /** Sort by a field, descending when {@code desc} is true. */
    public FindOptions sort(String field, boolean desc) {
        this.sortField = field;
        this.sortDesc = desc;
        return this;
    }

    /** Skip the first {@code n} results. */
    public FindOptions skip(int n) { this.skip = n; return this; }

    /** Return at most {@code n} results. */
    public FindOptions limit(int n) { this.limit = n; return this; }

    /** Group by a field; combine with the aggregation methods below. */
    public FindOptions group(String field) { this.groupField = field; return this; }

    /** Count documents per group; the output field is {@code count}. */
    public FindOptions count() { return agg("count", null); }

    /** Sum a field per group; the output field is {@code sum_<field>}. */
    public FindOptions sum(String field) { return agg("sum", field); }

    /** Average a field per group; the output field is {@code mean_<field>}. */
    public FindOptions mean(String field) { return agg("mean", field); }

    /** Smallest value per group; the output field is {@code min_<field>}. */
    public FindOptions min(String field) { return agg("min", field); }

    /** Largest value per group; the output field is {@code max_<field>}. */
    public FindOptions max(String field) { return agg("max", field); }

    /** All values per group as an array; output field {@code collect_<field>}. */
    public FindOptions collect(String field) { return agg("collect", field); }

    /** First value per group; the output field is {@code first_<field>}. */
    public FindOptions first(String field) { return agg("first", field); }

    /** Last value per group; the output field is {@code last_<field>}. */
    public FindOptions last(String field) { return agg("last", field); }

    /**
     * Add an aggregation by name. Prefer the named methods above; this exists
     * for functions added to the core after this binding was written.
     */
    public FindOptions agg(String func, String field) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("func", func);
        if (field != null) entry.put("field", field);
        aggs.add(entry);
        return this;
    }

    /** True when nothing has been set, so find() can take the plain path. */
    boolean isEmpty() {
        return sortField == null && skip == 0 && limit < 0
            && groupField == null && aggs.isEmpty();
    }

    String toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        if (sortField != null) {
            Map<String, Object> sort = new LinkedHashMap<>();
            sort.put("field", sortField);
            sort.put("desc", sortDesc);
            m.put("sort", sort);
        }
        if (skip > 0) m.put("skip", skip);
        if (limit >= 0) m.put("limit", limit);
        if (groupField != null) m.put("group", groupField);
        if (!aggs.isEmpty()) m.put("agg", aggs);
        return Json.write(m);
    }
}
