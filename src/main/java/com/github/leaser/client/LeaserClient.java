package com.github.leaser.client;

import com.github.leaser.lib.Lifecycle;
import com.github.leaser.rpc.AcquireLeaseRequest;
import com.github.leaser.rpc.AcquireLeaseResponse;
import com.github.leaser.rpc.ExtendLeaseRequest;
import com.github.leaser.rpc.ExtendLeaseResponse;
import com.github.leaser.rpc.GetExpiredLeasesRequest;
import com.github.leaser.rpc.GetExpiredLeasesResponse;
import com.github.leaser.rpc.GetLeaseInfoRequest;
import com.github.leaser.rpc.GetLeaseInfoResponse;
import com.github.leaser.rpc.GetRevokedLeasesRequest;
import com.github.leaser.rpc.GetRevokedLeasesResponse;
import com.github.leaser.rpc.RevokeLeaseRequest;
import com.github.leaser.rpc.RevokeLeaseResponse;

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
