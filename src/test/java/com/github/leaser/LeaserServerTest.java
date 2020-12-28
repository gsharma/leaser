package com.github.leaser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.github.leaser.LeaserServer.LeaserServerBuilder;

/**
 * Tests to keep the sanity of LeaserServer
 */
public final class LeaserServerTest {
    private static final Logger logger = LogManager.getLogger(LeaserServerTest.class.getSimpleName());

    @Test
    public void testLeaserServerLCM() throws Exception {
        final LeaserServer server = LeaserServerBuilder.newBuilder().serverHost("localhost").serverPort(7070).build();
        for (int iter = 0; iter < 3; iter++) {
            server.start();
            assertTrue(server.isRunning());
            server.stop();
            assertFalse(server.isRunning());
        }
    }

}
