package com.github.leaser;

import io.grpc.stub.StreamObserver;

public final class LeaserServiceImpl extends LeaserServiceGrpc.LeaserServiceImplBase {
    private final Leaser leaser;

    public LeaserServiceImpl(final Leaser leaser) {
        this.leaser = leaser;
    }

    @Override
    public void acquireLease(AcquireLeaseRequest request,
            StreamObserver<AcquireLeaseResponse> responseObserver) {
        // TODO: leaser.acquireLease(null, null, 0);
    }

    @Override
    public void revokeLease(RevokeLeaseRequest request,
            StreamObserver<RevokeLeaseResponse> responseObserver) {
        // TODO: leaser.revokeLease(null, null);
    }

    @Override
    public void extendLease(ExtendLeaseRequest request,
            StreamObserver<ExtendLeaseResponse> responseObserver) {
        // TODO: leaser.extendLease(null, null, 0);
    }

    @Override
    public void getLeaseInfo(GetLeaseInfoRequest request,
            StreamObserver<GetLeaseInfoResponse> responseObserver) {
        // TODO: leaser.getLeaseInfo(null, null);
    }

    @Override
    public void getExpiredLeases(GetExpiredLeasesRequest request,
            StreamObserver<GetExpiredLeasesResponse> responseObserver) {
        // TODO: leaser.getExpiredLeases();
    }

    @Override
    public void getRevokedLeases(GetRevokedLeasesRequest request,
            StreamObserver<GetRevokedLeasesResponse> responseObserver) {
        // TODO: leaser.getRevokedLeases();
    }

}
