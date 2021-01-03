package com.github.leaser.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.github.leaser.server.Leaser.LeaserMode;
import com.github.leaser.server.LeaserServer.LeaserServerBuilder;

/**
 * Tests to keep the sanity of LeaserServer
 */
public final class LeaserServerTest {
    private static final Logger logger = LogManager.getLogger(LeaserServerTest.class.getSimpleName());

    @Test
    public void testLeaserServerLCM() throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            final LeaserServer server = LeaserServerBuilder.newBuilder().serverHost("localhost").serverPort(7070)
                    .leaserMode(LeaserMode.PERSISTENT_ROCKSDB)
                    .maxTtlDaysAllowed(7L).auditorFrequencySeconds(1L).workerCount(4).build();
            server.start();
            assertTrue(server.isRunning());
            server.stop();
            assertFalse(server.isRunning());
        }
    }

    @Test
    public void testMultipleLeaserServers() throws Exception {
        final LeaserServer serverOne = LeaserServerBuilder.newBuilder().serverHost("localhost").serverPort(7070)
                .leaserMode(LeaserMode.PERSISTENT_ROCKSDB)
                .maxTtlDaysAllowed(7L).auditorFrequencySeconds(1L).workerCount(4).build();
        serverOne.start();
        assertTrue(serverOne.isRunning());

        final LeaserServer serverTwo = LeaserServerBuilder.newBuilder().serverHost("localhost").serverPort(7575)
                .leaserMode(LeaserMode.PERSISTENT_ROCKSDB)
                .maxTtlDaysAllowed(7L).auditorFrequencySeconds(1L).workerCount(4).build();
        serverTwo.start();
        assertTrue(serverTwo.isRunning());

        serverOne.stop();
        assertFalse(serverOne.isRunning());

        serverTwo.stop();
        assertFalse(serverTwo.isRunning());
    }

}
