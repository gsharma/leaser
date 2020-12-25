package com.github.leaser;

import java.util.Set;

import io.grpc.stub.StreamObserver;

public final class LeaserServiceImpl extends LeaserServiceGrpc.LeaserServiceImplBase {
    private final Leaser leaser;

    public LeaserServiceImpl(final Leaser leaser) {
        this.leaser = leaser;
    }

    @Override
    public void acquireLease(final AcquireLeaseRequest request,
            final StreamObserver<AcquireLeaseResponse> responseObserver) {
        try {
            final LeaseInfo leaseInfo = leaser.acquireLease(request.getOwnerId(), request.getResourceId(), request.getTtlSeconds());
            final Lease lease = leaseInfoToLease(leaseInfo);
            final AcquireLeaseResponse response = AcquireLeaseResponse.newBuilder().setLease(lease).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (LeaserException leaserProblem) {
            responseObserver.onError(leaserProblem);
        }
    }

    @Override
    public void revokeLease(final RevokeLeaseRequest request,
            final StreamObserver<RevokeLeaseResponse> responseObserver) {
        try {
            final boolean revoked = leaser.revokeLease(request.getOwnerId(), request.getResourceId());
            final RevokeLeaseResponse response = RevokeLeaseResponse.newBuilder().setRevoked(revoked).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (LeaserException leaserProblem) {
            responseObserver.onError(leaserProblem);
        }
    }

    @Override
    public void extendLease(final ExtendLeaseRequest request,
            final StreamObserver<ExtendLeaseResponse> responseObserver) {
        try {
            final LeaseInfo leaseInfo = leaser.extendLease(request.getOwnerId(), request.getResourceId(), request.getTtlExtendBySeconds());
            final Lease lease = leaseInfoToLease(leaseInfo);
            final ExtendLeaseResponse response = ExtendLeaseResponse.newBuilder().setLease(lease).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (LeaserException leaserProblem) {
            responseObserver.onError(leaserProblem);
        }
    }

    @Override
    public void getLeaseInfo(final GetLeaseInfoRequest request,
            final StreamObserver<GetLeaseInfoResponse> responseObserver) {
        try {
            final LeaseInfo leaseInfo = leaser.getLeaseInfo(request.getOwnerId(), request.getResourceId());
            final Lease lease = leaseInfoToLease(leaseInfo);
            final GetLeaseInfoResponse response = GetLeaseInfoResponse.newBuilder().setLease(lease).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (LeaserException leaserProblem) {
            responseObserver.onError(leaserProblem);
        }
    }

    @Override
    public void getExpiredLeases(final GetExpiredLeasesRequest request,
            final StreamObserver<GetExpiredLeasesResponse> responseObserver) {
        final Set<LeaseInfo> expiredLeaseInfos = leaser.getExpiredLeases();
        final GetExpiredLeasesResponse.Builder responseBuilder = GetExpiredLeasesResponse.newBuilder();
        if (expiredLeaseInfos != null && !expiredLeaseInfos.isEmpty()) {
            for (final LeaseInfo expiredLeaseInfo : expiredLeaseInfos) {
                responseBuilder.addExpiredLeases(leaseInfoToLease(expiredLeaseInfo));
            }
        }
        final GetExpiredLeasesResponse response = responseBuilder.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getRevokedLeases(final GetRevokedLeasesRequest request,
            final StreamObserver<GetRevokedLeasesResponse> responseObserver) {
        final Set<LeaseInfo> revokedLeaseInfos = leaser.getRevokedLeases();
        final GetRevokedLeasesResponse.Builder responseBuilder = GetRevokedLeasesResponse.newBuilder();
        if (revokedLeaseInfos != null && !revokedLeaseInfos.isEmpty()) {
            for (final LeaseInfo revokedLeaseInfo : revokedLeaseInfos) {
                responseBuilder.addRevokedLeases(leaseInfoToLease(revokedLeaseInfo));
            }
        }
        final GetRevokedLeasesResponse response = responseBuilder.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static Lease leaseInfoToLease(final LeaseInfo leaseInfo) {
        Lease lease = null;
        if (leaseInfo != null) {
            lease = Lease.newBuilder().setLeaseId(leaseInfo.getLeaseId()).setCreated(leaseInfo.getCreated().getTime())
                    .setOwnerId(leaseInfo.getOwnerId()).setResourceId(leaseInfo.getResourceId()).setTtlSeconds(leaseInfo.getTtlSeconds())
                    .setRevision(leaseInfo.getRevision()).setLastUpdated(leaseInfo.getLastUpdate().getTime())
                    .setExpirationEpochSeconds(leaseInfo.getExpirationEpochSeconds()).build();
        }
        return lease;
    }

}
