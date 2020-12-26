package com.github.leaser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

/**
 * Tests to keep the sanity of LeaserServer
 */
public final class LeaserServerTest {
    private static final Logger logger = LogManager.getLogger(LeaserServerTest.class.getSimpleName());

    @Test
    public void testLeaserServerLCM() throws Exception {
        final LeaserServer server = new LeaserServer();
        final Thread serverThread = new Thread() {
            @Override
            public void run() {
                try {
                    server.start();
                } catch (Exception serverProblem) {
                    try {
                        server.stop();
                    } catch (Exception problem) { // ignore
                    }
                }
            }
        };
        serverThread.setName("leaser-server");
        serverThread.setDaemon(true);
        serverThread.start();

        while (!server.isRunning()) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException interrupted) {
                break;
            }
        }
        assertTrue(server.isRunning());

        serverThread.interrupt();
        while (server.isRunning()) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException interrupted) {
                break;
            }
        }
        assertFalse(server.isRunning());
    }

}
