package com.github.leaser;

import java.util.Set;

/**
 * A simple service to manage resource leases.
 */
public interface Leaser {

    /**
     * Start the Leaser.
     * 
     * @throws LeaserException
     */
    void start() throws LeaserException;

    /**
     * Acquire a lease on the given resourceId to be held by ownerId for the duration of ttlSeconds. Unless the lease is extended via
     * {@link #extendLease(String, String, long)}, this lease expires ttlSeconds after creation.
     * 
     * @param ownerId
     * @param resourceId
     * @param ttlSeconds
     * @return
     * @throws LeaserException
     */
    LeaseInfo acquireLease(final String ownerId, final String resourceId, final long ttlSeconds) throws LeaserException;

    /**
     * Revoke a lease held by ownerId on resourceId.
     * 
     * @param ownerId
     * @param resourceId
     * @return
     * @throws LeaserException
     */
    boolean revokeLease(final String ownerId, final String resourceId) throws LeaserException;

    /**
     * Extend the lease duration held on resourceId by ownerId by an additional ttlExtendBySeconds. Return the LeaseInfo reflecting updated lease
     * metadata.
     * 
     * @param ownerId
     * @param resourceId
     * @param ttlExtendBySeconds
     * @return
     * @throws LeaserException
     */
    LeaseInfo extendLease(final String ownerId, final String resourceId, final long ttlExtendBySeconds) throws LeaserException;

    /**
     * Obtain the LeaseInfo as metadata for the lease held by ownerId on resourceId.
     * 
     * @param ownerId
     * @param resourceId
     * @return
     * @throws LeaserException
     */
    LeaseInfo getLeaseInfo(final String ownerId, final String resourceId) throws LeaserException;

    /**
     * Get all the expired leases still held in temporary buffer by the Leaser.
     * 
     * @return
     */
    Set<LeaseInfo> getExpiredLeases();

    /**
     * Get all the revoked leases still held in temporary buffer by the Leaser.
     * 
     * @return
     */
    Set<LeaseInfo> getRevokedLeases();

    /**
     * Stop the Leaser.
     * 
     * @throws LeaserException
     */
    void stop() throws LeaserException;

    /**
     * Check if the Leaser is running.
     * 
     * @return
     */
    boolean isRunning();

    /**
     * Factory method to create a MemoryLeaser instance.
     * 
     * @param maxTtlDaysAllowed
     * @param auditorFrequencySeconds
     * @return
     */
    static Leaser memoryLeaser(final long maxTtlDaysAllowed, final long auditorFrequencySeconds) {
        return new MemoryLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
    }

    /**
     * Factory method to create a PersistentLeaser instance.
     * 
     * @param maxTtlDaysAllowed
     * @param auditorFrequencySeconds
     * @return
     */
    static Leaser persistentLeaser(final long maxTtlDaysAllowed, final long auditorFrequencySeconds) {
        return new PersistentLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
    }

}
