package com.github.leaser;

/**
 * Client interface for the leaser service.
 */
public interface LeaserClient {

    AcquireLeaseResponse acquireLease(final AcquireLeaseRequest request);

    RevokeLeaseResponse revokeLease(final RevokeLeaseRequest request);

    ExtendLeaseResponse extendLease(final ExtendLeaseRequest request);

    GetLeaseInfoResponse getLeaseInfo(final GetLeaseInfoRequest request);

    GetExpiredLeasesResponse getExpiredLeases(final GetExpiredLeasesRequest request);

    GetRevokedLeasesResponse getRevokedLeases(final GetRevokedLeasesRequest request);

}
