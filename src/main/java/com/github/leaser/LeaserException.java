package com.github.leaser;

/**
 * Unified single exception that's thrown and handled by various components of Leaser. The idea is to use the code enum to encapsulate various
 * error/exception conditions. That said, stack traces, where available and desired, are not meant to be kept from users.
 */
public final class LeaserException extends Exception {
    private static final long serialVersionUID = 1L;
    private final Code code;

    public LeaserException(final Code code) {
        super(code.getDescription());
        this.code = code;
    }

    public LeaserException(final Code code, final String message) {
        super(message);
        this.code = code;
    }

    public LeaserException(final Code code, final Throwable throwable) {
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
