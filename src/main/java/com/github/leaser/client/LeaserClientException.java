package com.github.leaser.client;

/**
 * Unified single exception that's thrown and handled by various client-side components of Leaser. The idea is to use the code enum to encapsulate
 * various error/exception conditions. That said, stack traces, where available and desired, are not meant to be kept from users.
 */
public final class LeaserClientException extends Exception {
    private static final long serialVersionUID = 1L;
    private final Code code;

    public LeaserClientException(final Code code) {
        super(code.getDescription());
        this.code = code;
    }

    public LeaserClientException(final Code code, final String message) {
        super(message);
        this.code = code;
    }

    public LeaserClientException(final Code code, final Throwable throwable) {
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
        INVALID_LEASER_CLIENT_LCM("Leaser client cannot retransition to the same desired state"),
        // 3.
        LEASE_ALREADY_EXISTS("Lease already exists for the requested resource"),
        // 4.
        LEASE_NOT_FOUND("Lease for the requested resource does not exist"),
        // 5.
        LEASE_ALREADY_EXPIRED("Lease already expired for the requested resource"),
        // 6.
        LEASER_CLIENT_INIT_FAILURE("Failed to initialize the leaser client"),
        // 7.
        LEASER_CLIENT_TINI_FAILURE("Failed to cleanly shutdown the leaser client"),
        // 8.
        LEASER_INVALID_ARG("Invalid arguments passed"),
        // n.
        UNKNOWN_FAILURE(
                "Leaser client internal failure. Check exception stacktrace for more details of the failure");

        private String description;

        private Code(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

}
