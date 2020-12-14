package com.github.leaser;

/**
 * A simple service to manage resource leases.
 */
public interface Leaser {

    void start() throws LeaserException;

    LeaseInfo acquireLease(final String ownerId, final String resourceId, final long ttlSeconds) throws LeaserException;

    boolean revokeLease(final String ownerId, final String resourceId) throws LeaserException;

    LeaseInfo extendLease(final String ownerId, final String resourceId, final long ttlExtendBySeconds) throws LeaserException;

    LeaseInfo getLeaseInfo(final String ownerId, final String resourceId) throws LeaserException;

    void stop() throws LeaserException;

    static Leaser memoryLeaser(final long maxTtlDaysAllowed, final long auditorFrequencySeconds) {
        return new MemoryLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
    }

}
