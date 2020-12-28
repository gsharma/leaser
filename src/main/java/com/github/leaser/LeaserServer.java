package com.github.leaser;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.leaser.Leaser.LeaserMode;
import com.github.leaser.LeaserServerException.Code;

import io.grpc.Server;
//import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

/**
 * RPC Server for serving clients of leaser.
 */
public final class LeaserServer implements Lifecycle {
    private static final Logger logger = LogManager.getLogger(LeaserServer.class.getSimpleName());

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private final String serverHost;
    private final int serverPort;
    private final LeaserMode leaserMode;
    private final long maxTtlDaysAllowed;
    private final long auditorFrequencySeconds;

    private Leaser leaser;
    private Server server;
    private Thread serverThread;

    private LeaserServer(final String serverHost, final int serverPort, final LeaserMode leaserMode, final long maxTtlDaysAllowed,
            final long auditorFrequencySeconds) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.leaserMode = leaserMode;
        this.maxTtlDaysAllowed = maxTtlDaysAllowed;
        this.auditorFrequencySeconds = auditorFrequencySeconds;
    }

    public final static class LeaserServerBuilder {
        private String serverHost;
        private int serverPort;
        private LeaserMode leaserMode;
        private long maxTtlDaysAllowed;
        private long auditorFrequencySeconds;

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

        public LeaserServer build() throws LeaserServerException {
            if (serverHost == null || serverPort == 0 || leaserMode == null || maxTtlDaysAllowed == 0L || auditorFrequencySeconds == 0L) {
                throw new LeaserServerException(Code.LEASER_INIT_FAILURE,
                        "serverHost, serverPort, leaserMode, maxTtlDaysAllowed, auditorFrequencySeconds all need to be specified");
            }
            return new LeaserServer(serverHost, serverPort, leaserMode, maxTtlDaysAllowed, auditorFrequencySeconds);
        }

        private LeaserServerBuilder() {
        }
    }

    @Override
    public void start() throws Exception {
        final long startMillis = System.currentTimeMillis();
        logger.info("Starting leaser server [{}] at port {}", getIdentity().toString(), serverPort);
        final CountDownLatch serverReadyLatch = new CountDownLatch(1);
        if (running.compareAndSet(false, true)) {
            serverThread = new Thread() {
                {
                    setName("leaser-server");
                    setDaemon(true);
                }

                @Override
                public void run() {
                    try {
                        leaser = Leaser.getLeaser(leaserMode, maxTtlDaysAllowed, auditorFrequencySeconds);
                        leaser.start();
                        final LeaserServiceImpl service = new LeaserServiceImpl(leaser);
                        server = NettyServerBuilder.forAddress(new InetSocketAddress(serverHost, serverPort))
                                .addService(service).build();
                        server.start();
                        serverReadyLatch.countDown();
                        server.awaitTermination();
                    } catch (Exception serverProblem) {
                    }
                }
            };
            serverThread.start();
            if (serverReadyLatch.await(2L, TimeUnit.SECONDS)) {
                ready.set(true);
                logger.info("Started leaser server [{}] at port {} in {} millis", getIdentity().toString(), serverPort,
                        (System.currentTimeMillis() - startMillis));
            } else {
                logger.error("Failed to start leaser server [{}] at port {} in {} millis", getIdentity().toString(), serverPort,
                        (System.currentTimeMillis() - startMillis));
            }
        } else {
            logger.error("Invalid attempt to start an already running leaser server");
        }
    }

    @Override
    public void stop() throws Exception {
        final long startMillis = System.currentTimeMillis();
        logger.info("Stopping leaser server [{}]", getIdentity().toString());
        if (running.compareAndSet(true, false)) {
            ready.set(false);
            if (leaser.isRunning()) {
                leaser.stop();
            }
            if (!server.isTerminated()) {
                server.shutdown();
                server.awaitTermination(2L, TimeUnit.SECONDS);
                serverThread.interrupt();
            }
            logger.info("Stopped leaser server [{}] in {} millis", getIdentity().toString(), (System.currentTimeMillis() - startMillis));
        } else {
            logger.error("Invalid attempt to stop an already stopped leaser server");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }

    public static void main(String[] args) throws Exception {
        final LeaserServer leaserServer = new LeaserServer("localhost", 7070, LeaserMode.PERSISTENT_ROCKSDB, 7L, 1L);
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
