package com.github.leaser;

/**
 * Client interface for the leaser service.
 */
public interface LeaserClient extends Lifecycle {

    AcquireLeaseResponse acquireLease(final AcquireLeaseRequest request) throws LeaserClientException;

    RevokeLeaseResponse revokeLease(final RevokeLeaseRequest request) throws LeaserClientException;

    ExtendLeaseResponse extendLease(final ExtendLeaseRequest request) throws LeaserClientException;

    GetLeaseInfoResponse getLeaseInfo(final GetLeaseInfoRequest request) throws LeaserClientException;

    GetExpiredLeasesResponse getExpiredLeases(final GetExpiredLeasesRequest request) throws LeaserClientException;

    GetRevokedLeasesResponse getRevokedLeases(final GetRevokedLeasesRequest request) throws LeaserClientException;

    static LeaserClient getClient(final String serverHost, final int serverPort) {
        return new LeaserClientImpl(serverHost, serverPort);
    }

}
