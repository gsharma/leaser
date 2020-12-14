package com.github.leaser;

/**
 * A simple service to manage resource leases.
 */
public interface Leaser {

    void start() throws LeaserException;

    LeaseInfo acquireLease(Resource resource, long ttlSeconds) throws LeaserException;

    boolean revokeLease(String leaseId) throws LeaserException;

    LeaseInfo extendLease(String leaseId, long ttlExtendBySeconds) throws LeaserException;

    LeaseInfo getLeaseInfo(String leaseId) throws LeaserException;

    void stop() throws LeaserException;

}
