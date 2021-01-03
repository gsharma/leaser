package com.github.leaser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.github.leaser.client.LeaserClient;
import com.github.leaser.rpc.AcquireLeaseRequest;
import com.github.leaser.rpc.AcquireLeaseResponse;
import com.github.leaser.rpc.GetLeaseInfoRequest;
import com.github.leaser.rpc.GetLeaseInfoResponse;
import com.github.leaser.rpc.Lease;
import com.github.leaser.server.LeaserServer;
import com.github.leaser.server.Leaser.LeaserMode;
import com.github.leaser.server.LeaserServer.LeaserServerBuilder;

public final class LeaserPerfTest {
    private static final Logger logger = LogManager.getLogger(LeaserPerfTest.class.getSimpleName());

    {
        Thread.currentThread().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable problem) {
                logger.error("Unexpected error in thread {}", thread.getName(), problem);
            }
        });
    }

    @Test
    public void testManyClientsOneServer() throws Exception {
        final String serverHost = "localhost";
        final int serverPort = 7272;
        final long serverDeadlineSeconds = 1L;

        final int serverWorkerCount = 2;
        final int clientWorkerCount = 1;

        final int clientCount = 4;
        final List<LeaserClient> clients = new ArrayList<>(clientCount);

        LeaserServer server = null;
        try {
            server = LeaserServerBuilder.newBuilder().serverHost(serverHost).serverPort(serverPort)
                    .leaserMode(LeaserMode.PERSISTENT_ROCKSDB)
                    .maxTtlDaysAllowed(7L).auditorFrequencySeconds(1L).workerCount(serverWorkerCount).build();
            server.start();
            assertTrue(server.isRunning());

            for (int iter = 0; iter < clientCount; iter++) {
                final LeaserClient client = LeaserClient.getClient(serverHost, serverPort, serverDeadlineSeconds, clientWorkerCount);
                client.start();
                assertTrue(client.isRunning());
                clients.add(client);
            }

            for (int iter = 0; iter < clientCount; iter++) {
                final LeaserClient client = clients.get(iter);
                final String resourceId = "resource-" + iter;
                final String ownerId = "integ-test-" + iter;
                final long ttlSeconds = 2L;

                // 1. acquire lease
                final AcquireLeaseRequest acquireLeaseRequest = AcquireLeaseRequest.newBuilder().setResourceId(resourceId).setOwnerId(ownerId)
                        .setTtlSeconds(ttlSeconds).build();
                final long requestShipTime = System.currentTimeMillis();
                final AcquireLeaseResponse acquireLeaseResponse = client.acquireLease(acquireLeaseRequest);
                final Lease lease = acquireLeaseResponse.getLease();
                assertNotNull(lease.getLeaseId());
                assertEquals(resourceId, lease.getResourceId());
                assertEquals(ownerId, lease.getOwnerId());
                assertEquals(ttlSeconds, lease.getTtlSeconds());
                assertTrue(lease.getCreated() > requestShipTime);
                assertEquals(1L, lease.getRevision());
                assertEquals(0L, lease.getLastUpdated());

                // 2. get lease info
                final GetLeaseInfoRequest getLeaseInfoRequest = GetLeaseInfoRequest.newBuilder().setOwnerId(ownerId).setResourceId(resourceId)
                        .build();
                final GetLeaseInfoResponse getLeaseInfoResponse = client.getLeaseInfo(getLeaseInfoRequest);
                final Lease leaseRead = getLeaseInfoResponse.getLease();
                assertEquals(lease.getLeaseId(), leaseRead.getLeaseId());
                assertEquals(lease.getResourceId(), leaseRead.getResourceId());
                assertEquals(lease.getOwnerId(), leaseRead.getOwnerId());
            }
        } finally {
            for (final LeaserClient client : clients) {
                if (client != null) {
                    client.stop();
                    assertFalse(client.isRunning());
                }
            }
            if (server != null) {
                server.stop();
                assertFalse(server.isRunning());
            }
        }
    }

}
