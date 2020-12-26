package com.github.leaser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.github.leaser.LeaserException.Code;

/**
 * Tests to keep the sanity of Leaser
 */
public final class LeaserTest {
    private static final Logger logger = LogManager.getLogger(LeaserTest.class.getSimpleName());

    private enum Mode {
        PERSISTENT, MEMORY, PERSISTENTETCD
    };

    // Run any test with either Persistent or Memory leaser
    private static Leaser leaser(final Mode mode, final long maxTtlDaysAllowed, final long auditorFrequencySeconds) {
        Leaser leaser = null;
        switch (mode) {
            case PERSISTENT:
                leaser = Leaser.persistentLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
                break;
            case PERSISTENTETCD:
                leaser = Leaser.persistentLeaserEtcd(maxTtlDaysAllowed, auditorFrequencySeconds);
                break;
            case MEMORY:
                leaser = Leaser.memoryLeaser(maxTtlDaysAllowed, auditorFrequencySeconds);
                break;
        }
        return leaser;
    }

    @Test
    public void testLeaseAcquisition() throws Exception {
        Leaser leaser = null;
        try {
            leaser = leaser(Mode.PERSISTENT, 7L, 1L);
            leaser.start();
            int resourceCount = 1;
            final String ownerId = "unit-test";
            final long ttlSeconds = 1L;
            Resource resource = null;
            LeaseInfo leaseInfo = null;
            for (int iter = 0; iter < resourceCount; iter++) {
                resource = new Resource();
                leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
                assertEquals(leaseInfo, leaser.getLeaseInfo(ownerId, resource.getId()));
            }
            while (leaser.getLeaseInfo(ownerId, resource.getId()) != null) {
                Thread.sleep(500L);
            }
            assertNull(leaser.getLeaseInfo(ownerId, resource.getId()));
            assertTrue(leaser.getExpiredLeases().contains(leaseInfo));
            // now that resource is free to acquire a lease on, let's try once more
            leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
            assertEquals(leaseInfo, leaser.getLeaseInfo(ownerId, resource.getId()));
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }

    @Test
    public void testLeaseRevocation() throws Exception {
        Leaser leaser = null;
        try {
            leaser = leaser(Mode.PERSISTENT, 7L, 1L);
            leaser.start();
            final int resourceCount = 20;
            final String ownerId = "unit-test";
            final long ttlSeconds = 1L;
            int iter = 0;
            final List<String> testLeases = new ArrayList<String>();
            for (iter = 0; iter < resourceCount; iter++) {
                final Resource resource = new Resource();
                final LeaseInfo leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
                assertEquals(leaseInfo, leaser.getLeaseInfo(ownerId, resource.getId()));
                testLeases.add(resource.getId());
            }

            // remove a few random leases first
            int toRemove = 5;
            int expired = 0;
            Random random = new Random();
            for (int cont = 0; cont < toRemove; cont++) {
                iter = random.nextInt(resourceCount - cont - 1);
                final String resourceId = testLeases.get(iter);
                try {
                    assertTrue(leaser.revokeLease(ownerId, resourceId));
                } catch (LeaserException excepExpired) {
                    if (excepExpired.getCode() == Code.LEASE_ALREADY_EXPIRED)
                    {
                      logger.debug("expired!! code " + excepExpired.getCode()+ "  " + resourceId);
                      expired++;
                    }
                }
                assertNull(leaser.getLeaseInfo(ownerId, resourceId));
                testLeases.remove(iter);
            }
            assertEquals(toRemove-expired, leaser.getRevokedLeases().size());

            for (iter = testLeases.size() - 1; iter >= 0; iter--) {
                final String resourceId = testLeases.get(iter);
                try {
                    assertTrue(leaser.revokeLease(ownerId, resourceId));
                } catch (LeaserException excepExpired) {
                    if (excepExpired.getCode() == Code.LEASE_ALREADY_EXPIRED) 
                        logger.debug("expired!! code " + excepExpired.getCode()+ "  " + resourceId);
                }
                assertNull(leaser.getLeaseInfo(ownerId, resourceId));
                testLeases.remove(iter);
            }
            logger.debug("revoked " + leaser.getRevokedLeases().size() + " expired " + leaser.getExpiredLeases().size());
            assertEquals(resourceCount, leaser.getRevokedLeases().size() +
                    leaser.getExpiredLeases().size());
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
            leaser = leaser(Mode.PERSISTENT, 7L, 1L);
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
            // assertEquals(leaseInfo.getCreated(), extendedLeaseInfo.getCreated());
            assertEquals(2, extendedLeaseInfo.getRevision());
            assertNotNull(extendedLeaseInfo.getLastUpdate());
            assertTrue(extendedLeaseInfo.getExpirationEpochSeconds() > expirationSeconds);

            while (leaser.getLeaseInfo(ownerId, resource.getId()) != null) {
                Thread.sleep(500L);
            }
            assertNull(leaser.getLeaseInfo(ownerId, resource.getId()));
            assertTrue(leaser.getExpiredLeases().contains(leaseInfo));
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }

    @Test // (expected = LeaserException.class)
    public void testLeaserReinit() throws LeaserException {
        Leaser leaser = null;
        try {
            leaser = leaser(Mode.PERSISTENT, 7L, 1L);
            leaser.start();
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
            leaser = leaser(Mode.PERSISTENT, 7L, 1L);
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
        final Leaser leaser = leaser(Mode.PERSISTENT, 7L, 1L);
        for (int iter = 0; iter < 3; iter++) {
            leaser.start();
            leaser.stop();
        }
    }

    @Test
    public void testLeaseInfoComparison() throws LeaserException {
        Leaser leaser = null;
        try {
            leaser = leaser(Mode.PERSISTENT, 7L, 1L);
            leaser.start();
            Resource resourceOne = new Resource();
            long ttlSeconds = 2;
            String ownerId = "unit-test";
            LeaseInfo leaseInfoOne = leaser.acquireLease(ownerId, resourceOne.getId(), ttlSeconds);

            final Resource resourceTwo = new Resource();
            ttlSeconds = 2;
            ownerId = "unit-test";
            final LeaseInfo leaseInfoTwo = leaser.acquireLease(ownerId, resourceTwo.getId(), ttlSeconds);

            assertFalse(leaseInfoTwo.equals(leaseInfoOne));
            assertFalse(leaseInfoOne.compareTo(leaseInfoTwo) == 0);
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }

    @Test
    public void testLeaseInfoSerDe() throws LeaserException {
        Leaser leaser = null;
        try {
            leaser = leaser(Mode.PERSISTENT, 7L, 1L);
            leaser.start();
            Resource resource = new Resource();
            long ttlSeconds = 2000;
            String ownerId = "unit-test";
            LeaseInfo leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds);
            final byte[] serialized = LeaseInfo.serialize(leaseInfo);
            LeaseInfo serDeLeaseInfo = LeaseInfo.deserialize(serialized);
            assertTrue(serDeLeaseInfo.equals(leaseInfo));
        } finally {
            if (leaser != null) {
                leaser.stop();
            }
        }
    }
}
