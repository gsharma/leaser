package com.github.leaser;

import java.util.Set;

/**
 * A simple service to manage resource leases.
 */
public interface Leaser extends Lifecycle {

    /**
     * Acquire a lease on the given resourceId to be held by ownerId for the duration of ttlSeconds. Unless the lease is extended via
     * {@link #extendLease(String, String, long)}, this lease expires ttlSeconds after creation.
     * 
     * @param ownerId
     * @param resourceId
     * @param ttlSeconds
     * @return
     * @throws LeaserServerException
     */
    LeaseInfo acquireLease(final String ownerId, final String resourceId, final long ttlSeconds) throws LeaserServerException;

    /**
     * Revoke a lease held by ownerId on resourceId.
     * 
     * @param ownerId
     * @param resourceId
     * @return
     * @throws LeaserServerException
     */
    boolean revokeLease(final String ownerId, final String resourceId) throws LeaserServerException;

    /**
     * Extend the lease duration held on resourceId by ownerId by an additional ttlExtendBySeconds. Return the LeaseInfo reflecting updated lease
     * metadata.
     * 
     * @param ownerId
     * @param resourceId
     * @param ttlExtendBySeconds
     * @return
     * @throws LeaserServerException
     */
    LeaseInfo extendLease(final String ownerId, final String resourceId, final long ttlExtendBySeconds) throws LeaserServerException;

    /**
     * Obtain the LeaseInfo as metadata for the lease held by ownerId on resourceId.
     * 
     * @param ownerId
     * @param resourceId
     * @return
     * @throws LeaserServerException
     */
    LeaseInfo getLeaseInfo(final String ownerId, final String resourceId) throws LeaserServerException;

    /**
     * Get all the expired leases still held in temporary buffer by the Leaser.
     * 
     * @return
     * @throws LeaserServerException
     */
    Set<LeaseInfo> getExpiredLeases() throws LeaserServerException;

    /**
     * Get all the revoked leases still held in temporary buffer by the Leaser.
     * 
     * @return
     * @throws LeaserServerException
     */
    Set<LeaseInfo> getRevokedLeases() throws LeaserServerException;

    enum LeaserMode {
        PERSISTENT_ROCKSDB, MEMORY, PERSISTENT_ETCD
    };

    static Leaser getLeaser(final LeaserMode mode, final long maxTtlDaysAllowed, final long auditorFrequencySeconds) {
        Leaser leaser = null;
        switch (mode) {
            case PERSISTENT_ROCKSDB:
                leaser = new RocksdbPersistentLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
                break;
            case PERSISTENT_ETCD:
                leaser = new EtcdPersistentLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
                break;
            case MEMORY:
                leaser = new MemoryLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
                break;
        }
        return leaser;
    }

}
