package com.moofile;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Bindings to libmoofile through the JDK's Foreign Function &amp; Memory API.
 *
 * <p>Panama rather than JNI or JNR-FFI: JNI would need a hand-written C
 * shim exporting {@code Java_com_moofile_*} symbols (libmoofile exports plain
 * {@code moofile_*} ones), and JNR-FFI would add a third-party jar. Panama is
 * part of the JDK from 22 onward and needs neither.
 *
 * <p>Run with {@code --enable-native-access=ALL-UNNAMED} to silence the
 * restricted-method warning.
 *
 * <p>Package-private: callers use {@link Collection}.
 */
final class Native {

    private Native() {}

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LOOKUP;

    /** Arena for the library handle — lives as long as the JVM. */
    private static final Arena LIBRARY_ARENA = Arena.ofShared();

    static final ValueLayout.OfInt    C_INT  = ValueLayout.JAVA_INT;
    static final ValueLayout.OfLong   C_LONG = ValueLayout.JAVA_LONG;
    static final java.lang.foreign.AddressLayout PTR = ValueLayout.ADDRESS;

    /** A pointer that can be dereferenced — needed to read `char**` out-params. */
    static final java.lang.foreign.AddressLayout PTR_DEREF =
        ValueLayout.ADDRESS.withTargetLayout(
            java.lang.foreign.MemoryLayout.sequenceLayout(Long.MAX_VALUE, ValueLayout.JAVA_BYTE));

    static {
        LOOKUP = SymbolLookup.libraryLookup(resolveLibrary(), LIBRARY_ARENA);
    }

    /**
     * Locate libmoofile.
     *
     * <p>Order: the {@code moofile.library.path} system property, the
     * {@code MOOFILE_LIB} environment variable, then the in-repo cargo output
     * directories, then the platform's own search path.
     */
    private static String resolveLibrary() {
        String explicit = System.getProperty("moofile.library.path");
        if (explicit == null) explicit = System.getenv("MOOFILE_LIB");
        if (explicit != null && !explicit.isEmpty()) return explicit;

        String os = System.getProperty("os.name").toLowerCase();
        String name = os.contains("win") ? "moofile.dll"
                    : os.contains("mac") ? "libmoofile.dylib"
                    : "libmoofile.so";

        // src/main/java/com/moofile → repo root is five levels up from the
        // module dir; probe from the working directory outward.
        String[] roots = {
            "target/release", "target/debug",
            "../target/release", "../target/debug",
            "../../target/release", "../../target/debug",
            "../../../target/release", "../../../target/debug",
        };
        for (String root : roots) {
            Path p = Paths.get(root, name);
            if (Files.isRegularFile(p)) return p.toAbsolutePath().toString();
        }

        // Let the OS loader try; it throws a useful error if this fails.
        return name;
    }

    private static MethodHandle handle(String symbol, FunctionDescriptor descriptor) {
        MemorySegment address = LOOKUP.find(symbol).orElseThrow(
            () -> new MooFileException("symbol not found in libmoofile: " + symbol
                + " — is the library out of date? Rebuild with: "
                + "cargo build -p moofile-c --release"));
        return LINKER.downcallHandle(address, descriptor);
    }

    // Lifecycle
    static final MethodHandle OPEN = handle("moofile_open",
        FunctionDescriptor.of(PTR, PTR, PTR, PTR));
    static final MethodHandle CLOSE = handle("moofile_close",
        FunctionDescriptor.of(C_INT, PTR, PTR));

    // Insert
    static final MethodHandle INSERT = handle("moofile_insert",
        FunctionDescriptor.of(PTR_DEREF, PTR, PTR, PTR));
    static final MethodHandle INSERT_MANY = handle("moofile_insert_many",
        FunctionDescriptor.of(PTR_DEREF, PTR, PTR, PTR));

    // Query
    static final MethodHandle FIND = handle("moofile_find",
        FunctionDescriptor.of(PTR, PTR, PTR, PTR));
    static final MethodHandle FIND_EX = handle("moofile_find_ex",
        FunctionDescriptor.of(PTR, PTR, PTR, PTR, PTR));
    static final MethodHandle FIND_ONE = handle("moofile_find_one",
        FunctionDescriptor.of(PTR_DEREF, PTR, PTR, PTR));
    static final MethodHandle COUNT = handle("moofile_count",
        FunctionDescriptor.of(C_LONG, PTR, PTR, PTR));
    static final MethodHandle EXISTS = handle("moofile_exists",
        FunctionDescriptor.of(C_INT, PTR, PTR, PTR));

    // Cursor
    static final MethodHandle CURSOR_NEXT = handle("moofile_cursor_next",
        FunctionDescriptor.of(PTR_DEREF, PTR, PTR));
    static final MethodHandle CURSOR_FREE = handle("moofile_cursor_free",
        FunctionDescriptor.ofVoid(PTR));

    // Update
    static final MethodHandle UPDATE_ONE = handle("moofile_update_one",
        FunctionDescriptor.of(C_INT, PTR, PTR, PTR, PTR));
    static final MethodHandle UPDATE_MANY = handle("moofile_update_many",
        FunctionDescriptor.of(C_LONG, PTR, PTR, PTR, PTR));
    static final MethodHandle REPLACE_ONE = handle("moofile_replace_one",
        FunctionDescriptor.of(C_INT, PTR, PTR, PTR, PTR));

    // Delete
    static final MethodHandle DELETE_ONE = handle("moofile_delete_one",
        FunctionDescriptor.of(C_INT, PTR, PTR, PTR));
    static final MethodHandle DELETE_MANY = handle("moofile_delete_many",
        FunctionDescriptor.of(C_LONG, PTR, PTR, PTR));

    // Search
    static final MethodHandle VECTOR_SEARCH = handle("moofile_vector_search",
        FunctionDescriptor.of(PTR, PTR, PTR, PTR, PTR, C_INT, PTR));
    static final MethodHandle TEXT_SEARCH = handle("moofile_text_search",
        FunctionDescriptor.of(PTR, PTR, PTR, PTR, PTR, C_INT, PTR));
    static final MethodHandle HYBRID_SEARCH = handle("moofile_hybrid_search",
        FunctionDescriptor.of(PTR, PTR, PTR, PTR, PTR, PTR, PTR, C_INT, PTR));
    static final MethodHandle SEMANTIC_SEARCH = handle("moofile_semantic_search",
        FunctionDescriptor.of(PTR, PTR, PTR, PTR, PTR, C_INT, PTR));

    // Search cursor
    static final MethodHandle SEARCH_CURSOR_NEXT = handle("moofile_search_cursor_next",
        FunctionDescriptor.of(PTR_DEREF, PTR, PTR));
    static final MethodHandle SEARCH_CURSOR_FREE = handle("moofile_search_cursor_free",
        FunctionDescriptor.ofVoid(PTR));

    // Batch
    static final MethodHandle BATCH_BEGIN = handle("moofile_batch_begin",
        FunctionDescriptor.of(C_INT, PTR, PTR));
    static final MethodHandle BATCH_COMMIT = handle("moofile_batch_commit",
        FunctionDescriptor.of(C_INT, PTR, PTR));
    static final MethodHandle BATCH_ROLLBACK = handle("moofile_batch_rollback",
        FunctionDescriptor.of(C_INT, PTR, PTR));

    // Utility
    static final MethodHandle STATS = handle("moofile_stats",
        FunctionDescriptor.of(PTR_DEREF, PTR, PTR));
    static final MethodHandle COMPACT = handle("moofile_compact",
        FunctionDescriptor.of(C_INT, PTR, PTR));
    static final MethodHandle SYNC = handle("moofile_sync",
        FunctionDescriptor.of(C_INT, PTR, PTR));
    static final MethodHandle REINDEX = handle("moofile_reindex",
        FunctionDescriptor.of(C_INT, PTR, PTR));
    static final MethodHandle REEMBED = handle("moofile_reembed",
        FunctionDescriptor.of(C_LONG, PTR, PTR, PTR));

    // Memory
    static final MethodHandle FREE_STRING = handle("moofile_free_string",
        FunctionDescriptor.ofVoid(PTR));

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    /** Allocate a NUL-terminated C string, or NULL for a null argument. */
    static MemorySegment cString(Arena arena, String s) {
        return s == null ? MemorySegment.NULL : arena.allocateFrom(s);
    }

    /** Allocate the {@code char**} out-parameter, pre-set to NULL. */
    static MemorySegment errOut(Arena arena) {
        MemorySegment slot = arena.allocate(PTR);
        slot.set(PTR, 0, MemorySegment.NULL);
        return slot;
    }

    /**
     * Throw if the call reported an error, freeing the message either way.
     * The library NULLs the slot on entry, so a non-null value is always real.
     */
    static void checkError(MemorySegment errOut) {
        MemorySegment err = errOut.get(PTR_DEREF, 0);
        if (err == null || err.equals(MemorySegment.NULL)) return;
        String msg = err.getString(0);
        freeString(err);
        throw new MooFileException(msg);
    }

    /**
     * Copy an owned C string into Java and free it. Returns null for NULL,
     * which the C API uses for "no result" rather than for failure.
     */
    static String takeString(MemorySegment ptr) {
        if (ptr == null || ptr.equals(MemorySegment.NULL)) return null;
        String s = ptr.getString(0);
        freeString(ptr);
        return s;
    }

    static void freeString(MemorySegment ptr) {
        try {
            FREE_STRING.invokeExact(ptr);
        } catch (Throwable t) {
            throw new MooFileException("moofile_free_string failed: " + t, t);
        }
    }

    /** Wrap the checked Throwable that invokeExact declares. */
    static RuntimeException rethrow(Throwable t) {
        if (t instanceof MooFileException e) return e;
        if (t instanceof RuntimeException e) return e;
        return new MooFileException("native call failed: " + t, t);
    }
}
