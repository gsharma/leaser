package com.github.leaser.client;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.leaser.client.LeaserClientException.Code;
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
import com.github.leaser.rpc.LeaserServiceGrpc;
import com.github.leaser.rpc.RevokeLeaseRequest;
import com.github.leaser.rpc.RevokeLeaseResponse;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

/**
 * A simple leaser client implementation.
 */
final class LeaserClientImpl implements LeaserClient {
    private static final Logger logger = LogManager.getLogger(LeaserClientImpl.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();

    private final AtomicBoolean running;
    private final AtomicBoolean ready;

    private final String serverHost;
    private final int serverPort;
    private final long serverDeadlineSeconds;

    private ManagedChannel channel;
    private LeaserServiceGrpc.LeaserServiceBlockingStub serviceStub;
    private ThreadPoolExecutor clientExecutor;

    LeaserClientImpl(final String serverHost, final int serverPort, final long serverDeadlineSeconds) {
        this.running = new AtomicBoolean(false);
        this.ready = new AtomicBoolean(false);
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.serverDeadlineSeconds = serverDeadlineSeconds;
    }

    @Override
    public void start() throws LeaserClientException {
        if (running.compareAndSet(false, true)) {
            clientExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4, new ThreadFactory() {
                private final AtomicInteger threadIter = new AtomicInteger();
                private final String threadNamePattern = "leaser-client-%d";

                @Override
                public Thread newThread(final Runnable runnable) {
                    return new Thread(runnable, String.format(threadNamePattern, threadIter.incrementAndGet()));
                }
            });
            final ClientInterceptor deadlineInterceptor = new ClientInterceptor() {
                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                        final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
                    logger.debug("Intercepted {}", method.getFullMethodName());
                    return next.newCall(method, callOptions.withDeadlineAfter(serverDeadlineSeconds, TimeUnit.SECONDS));
                }
            };
            channel = ManagedChannelBuilder.forAddress(serverHost, serverPort).usePlaintext().executor(clientExecutor).intercept(deadlineInterceptor)
                    .userAgent("leaser-client").build();
            serviceStub = LeaserServiceGrpc.newBlockingStub(channel);
            // serviceStub = LeaserServiceGrpc.newBlockingStub(channel).withInterceptors(deadlineInterceptor).withExecutor(clientExecutor);
            // logger.info(serviceStub.getCallOptions().toString());
            ready.set(true);
            logger.info("Started LeaserClient [{}]", getIdentity());
        }
    }

    @Override
    public void stop() throws LeaserClientException {
        if (running.compareAndSet(true, false)) {
            try {
                ready.set(false);
                channel.shutdownNow().awaitTermination(1L, TimeUnit.SECONDS);
                channel.shutdown();
                if (clientExecutor != null && !clientExecutor.isTerminated()) {
                    clientExecutor.shutdown();
                    clientExecutor.awaitTermination(2L, TimeUnit.SECONDS);
                    clientExecutor.shutdownNow();
                    logger.info("Stopped leaser client worker threads");
                }
                logger.info("Stopped LeaserClient [{}]", getIdentity());
            } catch (Exception tiniProblem) {
                logger.error(tiniProblem);
            }
        }
    }

    @Override
    public String getIdentity() {
        return identity;
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
        AcquireLeaseResponse response = null;
        try {
            response = serviceStub.acquireLease(request);
        } catch (Throwable problem) {
            toLeaserClientException(problem);
        }
        return response;
    }

    @Override
    public RevokeLeaseResponse revokeLease(final RevokeLeaseRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        RevokeLeaseResponse response = null;
        try {
            response = serviceStub.revokeLease(request);
        } catch (Throwable problem) {
            toLeaserClientException(problem);
        }
        return response;
    }

    @Override
    public ExtendLeaseResponse extendLease(final ExtendLeaseRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        ExtendLeaseResponse response = null;
        try {
            response = serviceStub.extendLease(request);
        } catch (Throwable problem) {
            toLeaserClientException(problem);
        }
        return response;
    }

    @Override
    public GetLeaseInfoResponse getLeaseInfo(final GetLeaseInfoRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        GetLeaseInfoResponse response = null;
        try {
            response = serviceStub.getLeaseInfo(request);
        } catch (Throwable problem) {
            toLeaserClientException(problem);
        }
        return response;
    }

    @Override
    public GetExpiredLeasesResponse getExpiredLeases(final GetExpiredLeasesRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        GetExpiredLeasesResponse response = null;
        try {
            response = serviceStub.getExpiredLeases(request);
        } catch (Throwable problem) {
            toLeaserClientException(problem);
        }
        return response;
    }

    @Override
    public GetRevokedLeasesResponse getRevokedLeases(final GetRevokedLeasesRequest request) throws LeaserClientException {
        if (!isRunning()) {
            throw new LeaserClientException(Code.INVALID_LEASER_CLIENT_LCM, "Invalid attempt to operate an already stopped leaser client");
        }
        GetRevokedLeasesResponse response = null;
        try {
            response = serviceStub.getRevokedLeases(request);
        } catch (Throwable problem) {
            toLeaserClientException(problem);
        }
        return response;
    }

    private static void toLeaserClientException(final Throwable problem) throws LeaserClientException {
        if (problem instanceof StatusException) {
            final StatusException statusException = StatusException.class.cast(problem);
            final String status = statusException.getStatus().toString();
            throw new LeaserClientException(Code.LEASER_SERVER_ERROR, status, statusException);
        } else if (problem instanceof StatusRuntimeException) {
            final StatusRuntimeException statusRuntimeException = StatusRuntimeException.class.cast(problem);
            final String status = statusRuntimeException.getStatus().toString();
            throw new LeaserClientException(Code.LEASER_SERVER_ERROR, status, statusRuntimeException);
        } else if (problem instanceof LeaserClientException) {
            throw LeaserClientException.class.cast(problem);
        } else {
            throw new LeaserClientException(Code.UNKNOWN_FAILURE, problem);
        }
    }

}
