package com.github.leaser;

//import com.github.leaser.model.AcquireLeaseRequest;
//import com.github.leaser.model.AcquireLeaseResponse;
//import com.github.leaser.model.ExtendLeaseRequest;
//import com.github.leaser.model.ExtendLeaseResponse;
//import com.github.leaser.model.GetExpiredLeasesRequest;
//import com.github.leaser.model.GetExpiredLeasesResponse;
//import com.github.leaser.model.GetLeaseInfoRequest;
//import com.github.leaser.model.GetLeaseInfoResponse;
//import com.github.leaser.model.GetRevokedLeasesRequest;
//import com.github.leaser.model.GetRevokedLeasesResponse;
//import com.github.leaser.model.RevokeLeaseRequest;
//import com.github.leaser.model.RevokeLeaseResponse;

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
