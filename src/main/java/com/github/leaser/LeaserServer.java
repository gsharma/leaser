package com.github.leaser;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * RPC Server for serving clients of leaser.
 */
public final class LeaserServer implements Lifecycle {
    private static final Logger logger = LogManager.getLogger(LeaserServer.class.getSimpleName());

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private Leaser leaser;
    private Server server;
    private Thread serverThread;

    @Override
    public void start() throws Exception {
        final long startMillis = System.currentTimeMillis();
        int serverPort = 7070;
        long maxTtlDaysAllowed = 7L;
        long auditorFrequencySeconds = 1L;
        logger.info("Starting leaser server [{}] at port {}", getIdentity().toString(), serverPort);
        if (running.compareAndSet(false, true)) {
            leaser = Leaser.rocksdbPersistentLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
            leaser.start();
            final LeaserServiceImpl service = new LeaserServiceImpl(leaser);
            server = ServerBuilder.forPort(serverPort)
                    .addService(service).build();
            serverThread = new Thread() {
                @Override
                public void run() {
                    try {
                        server.start();
                        server.awaitTermination();
                    } catch (Exception serverProblem) {
                    }
                }
            };
            serverThread.setName("leaser-server");
            serverThread.setDaemon(true);
            serverThread.start();
            ready.set(true);
            logger.info("Started leaser server [{}] at port {} in {} millis", getIdentity().toString(), serverPort,
                    (System.currentTimeMillis() - startMillis));
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
        final LeaserServer leaserServer = new LeaserServer();
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
