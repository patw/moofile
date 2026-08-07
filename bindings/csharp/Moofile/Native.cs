using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Moofile;

/// <summary>
/// P/Invoke declarations for libmoofile.
/// </summary>
/// <remarks>
/// String arguments are marshalled as <c>LPUTF8Str</c>. The default for
/// <c>string</c> in a DllImport is ANSI, which mangles non-ASCII text on
/// Windows; the C API takes UTF-8 throughout.
///
/// Returned strings come back as <see cref="IntPtr"/> rather than
/// <c>string</c>: they are heap-allocated by Rust and must be released with
/// <see cref="moofile_free_string"/>, which the default string marshaller
/// would prevent by discarding the pointer.
/// </remarks>
internal static class Native
{
    private const string LibName = "moofile";

    static Native()
    {
        // Teach the runtime where to find the library before the first call.
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, Resolve);
    }

    /// <summary>Force the static constructor to run.</summary>
    internal static void EnsureLoaded() { }

    /// <summary>
    /// Locate libmoofile: the MOOFILE_LIB environment variable first, then
    /// the in-repo cargo output directories, then the platform's own search
    /// path (which covers NuGet's runtimes/ layout and system installs).
    /// </summary>
    private static IntPtr Resolve(string libraryName, System.Reflection.Assembly assembly,
                                  DllImportSearchPath? searchPath)
    {
        if (libraryName != LibName) return IntPtr.Zero;

        var explicitPath = Environment.GetEnvironmentVariable("MOOFILE_LIB");
        if (!string.IsNullOrEmpty(explicitPath) && File.Exists(explicitPath))
        {
            return NativeLibrary.Load(explicitPath);
        }

        string fileName =
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "moofile.dll" :
            RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "libmoofile.dylib" :
            "libmoofile.so";

        string[] roots =
        {
            AppContext.BaseDirectory,
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..", "target", "release"),
            Path.Combine("target", "release"),
            Path.Combine("..", "target", "release"),
            Path.Combine("..", "..", "target", "release"),
            Path.Combine("..", "..", "..", "target", "release"),
            Path.Combine("..", "..", "..", "target", "debug"),
        };

        foreach (var root in roots)
        {
            var candidate = Path.GetFullPath(Path.Combine(root, fileName));
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out var handle))
            {
                return handle;
            }
        }

        // Fall back to the default resolver.
        return IntPtr.Zero;
    }

    // Lifecycle
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_open(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string configJson,
        out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_close(IntPtr handle, out IntPtr errOut);

    // Insert
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_insert(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string docJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_insert_many(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string docsJson, out IntPtr errOut);

    // Query
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_find(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_find_ex(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string optionsJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_find_one(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_count(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_exists(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson, out IntPtr errOut);

    // Cursor
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_cursor_next(IntPtr cursor, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_cursor_free(IntPtr cursor);

    // Update
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_update_one(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string whereJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string updateJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_update_many(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string whereJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string updateJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_replace_one(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string whereJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string replJson, out IntPtr errOut);

    // Delete
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_delete_one(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string whereJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_delete_many(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string whereJson, out IntPtr errOut);

    // Search
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_vector_search(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string field,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string vecJson,
        int limit, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_text_search(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string field,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string query,
        int limit, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_hybrid_search(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string textField,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string vectorField,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string queryText,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? queryVectorJson,
        int limit, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_semantic_search(IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filterJson,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sourceField,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string queryText,
        int limit, out IntPtr errOut);

    // Search cursor
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_search_cursor_next(IntPtr cursor, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_search_cursor_free(IntPtr cursor);

    // Batch
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_begin(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_commit(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_rollback(IntPtr handle, out IntPtr errOut);

    // Utility
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_stats(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_compact(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_sync(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_reindex(IntPtr handle, out IntPtr errOut);

    // Memory
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_free_string(IntPtr s);

    // -----------------------------------------------------------------
    // Result helpers
    // -----------------------------------------------------------------

    /// <summary>
    /// Throw if the call reported an error, releasing the message either way.
    /// The library NULLs the slot on entry, so a non-zero value is always a
    /// real failure.
    /// </summary>
    internal static void ThrowIfError(IntPtr errPtr)
    {
        if (errPtr == IntPtr.Zero) return;
        var msg = Marshal.PtrToStringUTF8(errPtr) ?? "unknown MooFile error";
        moofile_free_string(errPtr);
        throw new MooFileException(msg);
    }

    /// <summary>
    /// Copy an owned C string into managed memory and release it. Returns
    /// null for NULL, which the C API uses for "no result", not for failure.
    /// </summary>
    internal static string? TakeString(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) return null;
        var s = Marshal.PtrToStringUTF8(ptr);
        moofile_free_string(ptr);
        return s;
    }
}
