package com.github.leaser;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.leaser.LeaserClientException.Code;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * A simple leaser client implementation.
 */
final class LeaserClientImpl implements LeaserClient {
    private static final Logger logger = LogManager.getLogger(LeaserClientImpl.class.getSimpleName());

    private final AtomicBoolean running;
    private final AtomicBoolean ready;

    private final String serverHost;
    private final int serverPort;

    private ManagedChannel channel;
    private LeaserServiceGrpc.LeaserServiceBlockingStub serviceStub;

    LeaserClientImpl(final String serverHost, final int serverPort) {
        this.running = new AtomicBoolean(false);
        this.ready = new AtomicBoolean(false);
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    @Override
    public void start() throws LeaserClientException {
        if (running.compareAndSet(false, true)) {
            channel = ManagedChannelBuilder.forAddress(serverHost, serverPort).usePlaintext().build();
            serviceStub = LeaserServiceGrpc.newBlockingStub(channel);
            ready.set(true);
            logger.info("Started LeaserClient [{}]", getIdentity().toString());
        }
    }

    @Override
    public void stop() throws LeaserClientException {
        if (running.compareAndSet(true, false)) {
            try {
                ready.set(false);
                channel.shutdownNow().awaitTermination(1L, TimeUnit.SECONDS);
                channel.shutdown();
                logger.info("Stopped LeaserClient [{}]", getIdentity().toString());
            } catch (Exception tiniProblem) {
                logger.error(tiniProblem);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }

    @Override
    public AcquireLeaseResponse acquireLease(final AcquireLeaseRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        return serviceStub.acquireLease(request);
    }

    @Override
    public RevokeLeaseResponse revokeLease(final RevokeLeaseRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        return serviceStub.revokeLease(request);
    }

    @Override
    public ExtendLeaseResponse extendLease(final ExtendLeaseRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        return serviceStub.extendLease(request);
    }

    @Override
    public GetLeaseInfoResponse getLeaseInfo(final GetLeaseInfoRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        return serviceStub.getLeaseInfo(request);
    }

    @Override
    public GetExpiredLeasesResponse getExpiredLeases(final GetExpiredLeasesRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        return serviceStub.getExpiredLeases(request);
    }

    @Override
    public GetRevokedLeasesResponse getRevokedLeases(final GetRevokedLeasesRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        return serviceStub.getRevokedLeases(request);
    }

}
