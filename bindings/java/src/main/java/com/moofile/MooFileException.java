package com.moofile;

/**
 * Raised for every MooFile failure — an error reported by the native library,
 * a malformed document, or use of a closed collection.
 *
 * <p>Unchecked, so it composes with lambdas passed to
 * {@link Collection#batch(Runnable)}.
 */
public class MooFileException extends RuntimeException {

    public MooFileException(String message) {
        super(message);
    }

    public MooFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
