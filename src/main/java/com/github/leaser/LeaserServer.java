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
public final class LeaserServer {
    private static final Logger logger = LogManager.getLogger(LeaserServer.class.getSimpleName());

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Leaser leaser;
    private Server server;

    public void start() throws Exception {
        int serverPort = 7070;
        long maxTtlDaysAllowed = 7L;
        long auditorFrequencySeconds = 1L;
        logger.info("Starting leaser server at port {}", serverPort);
        if (running.compareAndSet(false, true)) {
            leaser = Leaser.persistentLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
            leaser.start();
            final LeaserServiceImpl service = new LeaserServiceImpl(leaser);
            server = ServerBuilder.forPort(serverPort)
                    .addService(service).build();
            server.start();
            logger.info("Started leaser server at port {}", serverPort);
            server.awaitTermination();
        } else {
            logger.error("Invalid attempt to start an already running leaser server");
        }
    }

    public void stop() throws Exception {
        logger.info("Stopping leaser server");
        if (running.compareAndSet(true, false)) {
            leaser.stop();
            server.shutdown();
            server.awaitTermination(2L, TimeUnit.SECONDS);
            logger.info("Stopped leaser server");
        } else {
            logger.error("Invalid attempt to stop an already stopped leaser server");
        }
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
