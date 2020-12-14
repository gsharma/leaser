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
    public void testLeaseAcquisition() throws Exception {
        Leaser leaser = null;
        try {
            leaser = Leaser.memoryLeaser(7L, 1L);
            leaser.start();
            final Resource resource = new Resource();
            final long ttlSeconds = 1L;
            final String ownerId = "unit-test";
            LeaseInfo leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
            assertEquals(leaseInfo, leaser.getLeaseInfo(ownerId, resource.getId()));

            while (MemoryLeaser.class.cast(leaser).getExpiredLeases().size() != 1) {
                Thread.sleep(TimeUnit.MILLISECONDS.convert(1L, TimeUnit.SECONDS));
            }
            assertNull(leaser.getLeaseInfo(ownerId, resource.getId()));
            assertTrue(MemoryLeaser.class.cast(leaser).getExpiredLeases().contains(leaseInfo));

            // now that resource is free to acquire a lease on, let's try once more
            leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
            assertEquals(leaseInfo, leaser.getLeaseInfo(ownerId, resource.getId()));

            while (MemoryLeaser.class.cast(leaser).getExpiredLeases().size() != 2) {
                Thread.sleep(TimeUnit.MILLISECONDS.convert(1L, TimeUnit.SECONDS));
            }
            assertNull(leaser.getLeaseInfo(ownerId, resource.getId()));
            assertTrue(MemoryLeaser.class.cast(leaser).getExpiredLeases().contains(leaseInfo));
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }

    @Test
    public void testLeaseExtension() throws Exception {
        Leaser leaser = null;
        try {
            leaser = Leaser.memoryLeaser(7L, 1L);
            leaser.start();
            final Resource resource = new Resource();
            final long ttlSeconds = 1L;
            final String ownerId = "unit-test";
            final LeaseInfo leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
            assertEquals(leaseInfo, leaser.getLeaseInfo(ownerId, resource.getId()));
            assertEquals(1, leaseInfo.getRevision());
            assertNull(leaseInfo.getLastUpdate());
            long expirationSeconds = leaseInfo.getExpirationEpochSeconds();

            final LeaseInfo extendedLeaseInfo = leaser.extendLease(ownerId, resource.getId(), 1L);
            assertEquals(leaseInfo, extendedLeaseInfo);
            assertEquals(leaseInfo.getCreated(), extendedLeaseInfo.getCreated());
            assertEquals(2, extendedLeaseInfo.getRevision());
            assertNotNull(extendedLeaseInfo.getLastUpdate());
            assertTrue(extendedLeaseInfo.getExpirationEpochSeconds() > expirationSeconds);

            while (MemoryLeaser.class.cast(leaser).getExpiredLeases().size() != 1) {
                Thread.sleep(TimeUnit.MILLISECONDS.convert(1L, TimeUnit.SECONDS));
            }
            assertNull(leaser.getLeaseInfo(ownerId, resource.getId()));
            assertTrue(MemoryLeaser.class.cast(leaser).getExpiredLeases().contains(leaseInfo));
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }

    @Test
    public void testLeaseRevocation() throws Exception {
        // TODO: Denise to fill out
    }

    @Test // (expected = LeaserException.class)
    public void testLeaserReinit() throws LeaserException {
        Leaser leaser = null;
        try {
            leaser = Leaser.memoryLeaser(7L, 1L);
            leaser.start();
            assertTrue(MemoryLeaser.class.cast(leaser).getExpiredLeases().size() != 1);
            // should blow-up
            leaser.start();
        } catch (LeaserException alreadyStartedException) {
            assertEquals(LeaserException.Code.INVALID_LEASER_LCM, alreadyStartedException.getCode());
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }

    @Test
    public void testIllegalLeaseAcquisition() throws LeaserException {
        Leaser leaser = null;
        try {
            leaser = Leaser.memoryLeaser(7L, 1L);
            leaser.start();
            final Resource resource = new Resource();
            final long ttlSeconds = 2;
            final String ownerId = "unit-test";
            final LeaseInfo leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
            assertEquals(leaseInfo, leaser.getLeaseInfo(ownerId, resource.getId()));
            assertEquals(1, leaseInfo.getRevision());
            assertNull(leaseInfo.getLastUpdate());
        } catch (LeaserException leaseAlreadyTakenException) {
            assertEquals(LeaserException.Code.LEASE_ALREADY_EXISTS, leaseAlreadyTakenException.getCode());
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }

    @Test
    public void testLeaserLCM() throws LeaserException {
        final Leaser leaser = Leaser.memoryLeaser(7L, 1L);
        for (int iter = 0; iter < 3; iter++) {
            leaser.start();
            leaser.stop();
        }
    }
}
