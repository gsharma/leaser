package com.github.leaser;

/**
 * A simple service to manage resource leases.
 */
public interface Leaser {

    void start() throws LeaserException;

    LeaseInfo acquireLease(String resourceId, long ttlSeconds) throws LeaserException;

    boolean revokeLease(String resourceId) throws LeaserException;

    LeaseInfo extendLease(String resourceId, long ttlExtendBySeconds) throws LeaserException;

    LeaseInfo getLeaseInfo(String resourceId) throws LeaserException;

    void stop() throws LeaserException;

    static Leaser memoryLeaser(final long maxTtlDaysAllowed, final long auditorFrequencySeconds) {
        return new MemoryLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
    }

}
