package com.moofile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * What a {@link Collection#repair(String)} pass did.
 *
 * <p>A repair keeps every record that still decodes and drops the byte spans
 * that do not — resynchronising past damage where an intact record follows it
 * and truncating where none does.  {@link #gaps()} lists what was dropped, so
 * a caller can log or alert on data loss rather than discover it later.
 */
public record RepairReport(
    /** Records that decoded and were preserved. */
    long recordsKept,
    /** Bytes of intact records preserved. */
    long bytesKept,
    /** Bytes of unparseable data dropped. */
    long bytesDropped,
    /** False when the file was already intact and was left untouched. */
    boolean rewritten,
    /** Every damaged span, in file order. */
    List<Gap> gaps
) {
    /** A span of bytes a repair could not parse and dropped. */
    public record Gap(
        /** Byte offset where the damage starts. */
        long offset,
        /** Number of bytes dropped. */
        long length,
        /**
         * Whether the damage ran to the end of the file — i.e. this was a
         * truncation rather than a skipped-over hole.
         */
        boolean toEndOfFile
    ) {}

    /** Whether any damage was found. */
    public boolean isDamaged() { return !gaps.isEmpty(); }

    /** Parse the JSON report the C layer returns. */
    static RepairReport fromJson(String json) {
        Document d = Document.parse(json);
        List<Gap> gaps = new ArrayList<>();
        List<Object> raw = d.getList("gaps");
        if (raw != null) {
            for (Object o : raw) {
                if (!(o instanceof Map<?, ?> m)) continue;
                @SuppressWarnings("unchecked")
                Document g = new Document((Map<String, Object>) m);
                gaps.add(new Gap(
                    g.getLong("offset"),
                    g.getLong("length"),
                    g.getBoolean("to_end_of_file")));
            }
        }
        return new RepairReport(
            d.getLong("records_kept"),
            d.getLong("bytes_kept"),
            d.getLong("bytes_dropped"),
            d.getBoolean("rewritten"),
            List.copyOf(gaps));
    }
}
