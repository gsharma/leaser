package com.github.leaser.server;

import java.util.Set;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

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
import com.github.leaser.rpc.Lease;
import com.github.leaser.rpc.LeaserServiceGrpc;
import com.github.leaser.rpc.RevokeLeaseRequest;
import com.github.leaser.rpc.RevokeLeaseResponse;

/**
 * RPC Service implementation for the Leaser.
 */
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
        } catch (LeaserServerException leaserProblem) {
            responseObserver.onError(toStatusRuntimeException(leaserProblem));
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
        } catch (LeaserServerException leaserProblem) {
            responseObserver.onError(toStatusRuntimeException(leaserProblem));
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
        } catch (LeaserServerException leaserProblem) {
            responseObserver.onError(toStatusRuntimeException(leaserProblem));
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
        } catch (LeaserServerException leaserProblem) {
            responseObserver.onError(toStatusRuntimeException(leaserProblem));
        }
    }

    @Override
    public void getExpiredLeases(final GetExpiredLeasesRequest request,
            final StreamObserver<GetExpiredLeasesResponse> responseObserver) {
        try {
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
        } catch (LeaserServerException leaserProblem) {
            responseObserver.onError(toStatusRuntimeException(leaserProblem));
        }
    }

    @Override
    public void getRevokedLeases(final GetRevokedLeasesRequest request,
            final StreamObserver<GetRevokedLeasesResponse> responseObserver) {
        try {
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
        } catch (LeaserServerException leaserProblem) {
            responseObserver.onError(toStatusRuntimeException(leaserProblem));
        }
    }

    private static Lease leaseInfoToLease(final LeaseInfo leaseInfo) {
        Lease lease = null;
        if (leaseInfo != null) {
            lease = Lease.newBuilder().setLeaseId(leaseInfo.getLeaseId()).setCreated(leaseInfo.getCreated().getTime())
                    .setOwnerId(leaseInfo.getOwnerId()).setResourceId(leaseInfo.getResourceId()).setTtlSeconds(leaseInfo.getTtlSeconds())
                    .setRevision(leaseInfo.getRevision())
                    .setLastUpdated(leaseInfo.getLastUpdate() != null ? leaseInfo.getLastUpdate().getTime() : 0L)
                    .setExpirationEpochSeconds(leaseInfo.getExpirationEpochSeconds()).build();
        }
        return lease;
    }

    private static StatusRuntimeException toStatusRuntimeException(final LeaserServerException serverException) {
        return new StatusRuntimeException(Status.fromCode(Code.INTERNAL).withCause(serverException).withDescription(serverException.getMessage()));
    }

}
