package com.github.leaser.server;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.leaser.lib.Lifecycle;
import com.github.leaser.server.Leaser.LeaserMode;
import com.github.leaser.server.LeaserServerException.Code;

import io.grpc.Server;
//import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;

/**
 * RPC Server for serving clients of leaser.
 */
public final class LeaserServer implements Lifecycle {
    private static final Logger logger = LogManager.getLogger(LeaserServer.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private final String serverHost;
    private final int serverPort;
    private final LeaserMode leaserMode;
    private final long maxTtlDaysAllowed;
    private final long auditorFrequencySeconds;
    private final int workerCount;

    private Leaser leaser;
    private Server server;
    private Thread serverThread;
    private ThreadPoolExecutor serverExecutor;

    private LeaserServer(final String serverHost, final int serverPort, final LeaserMode leaserMode, final long maxTtlDaysAllowed,
            final long auditorFrequencySeconds, final int workerCount) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.leaserMode = leaserMode;
        this.maxTtlDaysAllowed = maxTtlDaysAllowed;
        this.auditorFrequencySeconds = auditorFrequencySeconds;
        this.workerCount = workerCount;
    }

    public final static class LeaserServerBuilder {
        private String serverHost;
        private int serverPort;
        private LeaserMode leaserMode;
        private long maxTtlDaysAllowed;
        private long auditorFrequencySeconds;
        private int workerCount;

        public static LeaserServerBuilder newBuilder() {
            return new LeaserServerBuilder();
        }

        public LeaserServerBuilder serverHost(final String serverHost) {
            this.serverHost = serverHost;
            return this;
        }

        public LeaserServerBuilder serverPort(final int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public LeaserServerBuilder leaserMode(final LeaserMode leaserMode) {
            this.leaserMode = leaserMode;
            return this;
        }

        public LeaserServerBuilder maxTtlDaysAllowed(final long maxTtlDaysAllowed) {
            this.maxTtlDaysAllowed = maxTtlDaysAllowed;
            return this;
        }

        public LeaserServerBuilder auditorFrequencySeconds(final long auditorFrequencySeconds) {
            this.auditorFrequencySeconds = auditorFrequencySeconds;
            return this;
        }

        public LeaserServerBuilder workerCount(final int workerCount) {
            this.workerCount = workerCount;
            return this;
        }

        public LeaserServer build() throws LeaserServerException {
            if (serverHost == null || serverPort == 0 || leaserMode == null || maxTtlDaysAllowed == 0L || auditorFrequencySeconds == 0L
                    || workerCount == 0) {
                throw new LeaserServerException(Code.LEASER_INIT_FAILURE,
                        "serverHost, serverPort, leaserMode, maxTtlDaysAllowed, auditorFrequencySeconds, workerCount all need to be specified");
            }
            return new LeaserServer(serverHost, serverPort, leaserMode, maxTtlDaysAllowed, auditorFrequencySeconds, workerCount);
        }

        private LeaserServerBuilder() {
        }
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public void start() throws Exception {
        if (running.compareAndSet(false, true)) {
            final CountDownLatch serverReadyLatch = new CountDownLatch(1);
            serverThread = new Thread() {
                {
                    setName("leaser-server-main");
                    setDaemon(true);
                }

                @Override
                public void run() {
                    final long startNanos = System.nanoTime();
                    logger.info("Starting LeaserServer [{}] at port {}", getIdentity(), serverPort);
                    try {
                        leaser = Leaser.getLeaser(leaserMode, maxTtlDaysAllowed, auditorFrequencySeconds);
                        leaser.start();

                        serverExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(workerCount, new ThreadFactory() {
                            private final AtomicInteger threadIter = new AtomicInteger();
                            private final String threadNamePattern = "leaser-server-%d";

                            @Override
                            public Thread newThread(final Runnable runnable) {
                                final Thread worker = new Thread(runnable, String.format(threadNamePattern, threadIter.incrementAndGet()));
                                worker.setDaemon(true);
                                return worker;
                            }
                        });
                        final LeaserServiceImpl service = new LeaserServiceImpl(leaser);
                        server = NettyServerBuilder.forAddress(new InetSocketAddress(serverHost, serverPort))
                                .addService(service).intercept(TransmitStatusRuntimeExceptionInterceptor.instance()).executor(serverExecutor).build();
                        server.start();
                        serverReadyLatch.countDown();
                        logger.info("Started LeaserServer [{}] at port {} in {} millis", getIdentity(), serverPort,
                                TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));
                        server.awaitTermination();
                    } catch (Exception serverProblem) {
                        logger.error("Failed to start LeaserServer [{}] at port {} in {} millis", getIdentity(), serverPort,
                                TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS), serverProblem);
                    }
                }
            };
            serverThread.start();
            if (serverReadyLatch.await(2L, TimeUnit.SECONDS)) {
                ready.set(true);
            } else {
                logger.error("Failed to start LeaserServer [{}] at port", getIdentity(), serverPort);
            }
        } else {
            logger.error("Invalid attempt to start an already running LeaserServer");
        }
    }

    @Override
    public void stop() throws Exception {
        final long startNanos = System.nanoTime();
        logger.info("Stopping LeaserServer [{}]", getIdentity());
        if (running.compareAndSet(true, false)) {
            ready.set(false);
            if (!server.isTerminated()) {
                server.shutdown();
                server.awaitTermination(2L, TimeUnit.SECONDS);
                serverThread.interrupt();
                logger.info("Stopped leaser server main thread");
            }
            if (serverExecutor != null && !serverExecutor.isTerminated()) {
                serverExecutor.shutdown();
                serverExecutor.awaitTermination(2L, TimeUnit.SECONDS);
                serverExecutor.shutdownNow();
                logger.info("Stopped leaser server worker threads");
            }
            if (leaser.isRunning()) {
                leaser.stop();
            }
            logger.info("Stopped LeaserServer [{}] in {} millis", getIdentity(),
                    TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));
        } else {
            logger.error("Invalid attempt to stop an already stopped LeaserServer");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }

    public static void main(String[] args) throws Exception {
        final LeaserServer leaserServer = new LeaserServer("localhost", 7070, LeaserMode.PERSISTENT_ROCKSDB, 7L, 1L, 8);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    leaserServer.stop();
                } catch (Exception stoppageProblem) {
                    stoppageProblem.printStackTrace();
                }
            }
        });
        leaserServer.start();
    }

}
