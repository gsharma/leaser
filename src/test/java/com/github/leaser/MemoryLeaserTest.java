package com.github.leaser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

/**
 * Tests to keep the sanity of MemoryLeaser
 */
public class MemoryLeaserTest {
    private static final Logger logger = LogManager.getLogger(MemoryLeaserTest.class.getSimpleName());

    @Test
    public void testSimpleLease() throws Exception {
        final MemoryLeaser leaser = new MemoryLeaser();
        leaser.start();
        final Resource resource = new Resource();
        final long ttlSeconds = 2;
        final LeaseInfo leaseInfo = leaser.acquireLease(resource, ttlSeconds);
        assertEquals(leaseInfo, leaser.getLeaseInfo(leaseInfo.getLeaseId()));

        while (leaser.getExpiredLeases().isEmpty()) {
            Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));
        }
        assertNull(leaser.getLeaseInfo(leaseInfo.getLeaseId()));
        assertTrue(leaser.getExpiredLeases().contains(leaseInfo));
        leaser.stop();
    }

    @Test
    public void testLeaseExtension() throws Exception {
        final MemoryLeaser leaser = new MemoryLeaser();
        leaser.start();
        final Resource resource = new Resource();
        final long ttlSeconds = 2;
        final LeaseInfo leaseInfo = leaser.acquireLease(resource, ttlSeconds);
        assertEquals(leaseInfo, leaser.getLeaseInfo(leaseInfo.getLeaseId()));
        assertEquals(1, leaseInfo.getRevision());
        assertNull(leaseInfo.getLastUpdate());
        long expirationSeconds = leaseInfo.getExpirationEpochSeconds();

        final LeaseInfo extendedLeaseInfo = leaser.extendLease(leaseInfo.getLeaseId(), 1L);
        assertEquals(leaseInfo, extendedLeaseInfo);
        assertEquals(leaseInfo.getCreated(), extendedLeaseInfo.getCreated());
        assertEquals(2, extendedLeaseInfo.getRevision());
        assertNotNull(extendedLeaseInfo.getLastUpdate());
        assertTrue(extendedLeaseInfo.getExpirationEpochSeconds() > expirationSeconds);

        while (leaser.getExpiredLeases().isEmpty()) {
            Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));
        }
        assertNull(leaser.getLeaseInfo(leaseInfo.getLeaseId()));
        assertTrue(leaser.getExpiredLeases().contains(leaseInfo));
        leaser.stop();
    }

    @Test
    public void testLeaseRevocation() throws Exception {
        // TODO: Denise to fill out
    }

    @Test(expected = LeaserException.class)
    public void testLeaserLCM() throws LeaserException {
        final MemoryLeaser leaser = new MemoryLeaser();
        try {
            leaser.start();
        } catch (LeaserException exception) {
            // ignore this one
        }
        assertTrue(leaser.getExpiredLeases().isEmpty());
        // should blow-up
        leaser.start();
    }

}
