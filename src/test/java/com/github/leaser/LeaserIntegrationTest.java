package com.github.leaser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.github.leaser.Leaser.LeaserMode;
import com.github.leaser.LeaserServer.LeaserServerBuilder;

/**
 * End to end tests for keeping leaser's sanity.
 */
public class LeaserIntegrationTest {
    private static final Logger logger = LogManager.getLogger(LeaserIntegrationTest.class.getSimpleName());

    {
        Thread.currentThread().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable problem) {
                logger.error("Unexpected error in thread {}", thread.getName(), problem);
            }
        });
    }

    @Test
    public void testLeaseAcquisition() throws Exception {
        final String serverHost = "localhost";
        final int serverPort = 7272;
        LeaserServer server = null;
        LeaserClient client = null;
        try {
            server = LeaserServerBuilder.newBuilder().serverHost(serverHost).serverPort(serverPort)
                    .leaserMode(LeaserMode.PERSISTENT_ROCKSDB)
                    .maxTtlDaysAllowed(7L).auditorFrequencySeconds(1L).build();
            server.start();
            assertTrue(server.isRunning());

            client = LeaserClient.getClient(serverHost, serverPort);
            client.start();
            assertTrue(client.isRunning());

            final String resourceId = "resource-1";
            final String ownerId = "integ-test";
            final long ttlSeconds = 2L;
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
        } finally {
            if (client != null) {
                client.stop();
                assertFalse(client.isRunning());
            }
            if (server != null) {
                server.stop();
                assertFalse(server.isRunning());
            }
        }
    }

}
