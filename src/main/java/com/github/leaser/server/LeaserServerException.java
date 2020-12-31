package com.github.leaser.server;

/**
 * Unified single exception that's thrown and handled by various server-side components of Leaser. The idea is to use the code enum to encapsulate
 * various error/exception conditions. That said, stack traces, where available and desired, are not meant to be kept from users.
 */
public final class LeaserServerException extends Exception {
    private static final long serialVersionUID = 1L;
    private final Code code;

    public LeaserServerException(final Code code) {
        super(code.getDescription());
        this.code = code;
    }

    public LeaserServerException(final Code code, final String message) {
        super(message);
        this.code = code;
    }

    public LeaserServerException(final Code code, final Throwable throwable) {
        super(throwable);
        this.code = code;
    }

    public Code getCode() {
        return code;
    }

    public static enum Code {
        // 1.
        INVALID_LEASE_TTL("Lease TTL should be greater than zero seconds"),
        // 2.
        INVALID_LEASER_LCM("Leaser cannot retransition to the same desired state"),
        // 3.
        LEASE_ALREADY_EXISTS("Lease already exists for the requested resource"),
        // 4.
        LEASE_NOT_FOUND("Lease for the requested resource does not exist"),
        // 5.
        LEASE_ALREADY_EXPIRED("Lease already expired for the requested resource"),
        // 6.
        LEASER_INIT_FAILURE("Failed to initialize the leaser"),
        // 7.
        LEASE_PERSISTENCE_FAILURE("Issue encountered with lease persistence"),
        // 8.
        LEASER_TINI_FAILURE("Failed to cleanly shutdown the leaser"),
        // 9.
        LEASER_INVALID_ARG("Invalid arguments passed"),
        // n.
        UNKNOWN_FAILURE(
                "Leaser internal failure. Check exception stacktrace for more details of the failure");

        private String description;

        private Code(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

}
