package sis2.core;

/** Transport-independent business error; each transport maps {@link Kind} to its own status code. */
public class ServiceException extends RuntimeException {

    public enum Kind {
        /** Bad request. NOT retryable (gRPC INVALID_ARGUMENT / HTTP 400). */
        INVALID_ARGUMENT,
        /** Same idempotency key reused with a different payload. NOT retryable (gRPC FAILED_PRECONDITION / HTTP 422). */
        KEY_REUSED,
        /** Transient failure. Retryable (gRPC UNAVAILABLE / HTTP 503). */
        UNAVAILABLE,
        /** Caller went away (deadline exceeded / cancelled) before the side effect was committed. */
        CANCELLED
    }

    private static final long serialVersionUID = 1L;
    private final Kind kind;

    public ServiceException(Kind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
