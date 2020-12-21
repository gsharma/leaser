package com.github.leaser;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.leaser.LeaserException.Code;

/**
 * An in-memory implementation for the Leaser.
 */
public final class MemoryLeaser implements Leaser {
    private static final Logger logger = LogManager.getLogger(Leaser.class.getSimpleName());

    private final String identity;
    private final AtomicBoolean running;
    private final ConcurrentMap<String, LeaseInfo> liveLeases = new ConcurrentHashMap<>();

    private static final int expiredLeasesToKeep = 25;
    private final Set<LeaseInfo> expiredLeases = Collections.newSetFromMap(new LinkedHashMap<LeaseInfo, Boolean>() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean removeEldestEntry(final Map.Entry<LeaseInfo, Boolean> eldest) {
            return size() > expiredLeasesToKeep;
        }
    });
    private static final int revokedLeasesToKeep = 25;
    private final Set<LeaseInfo> revokedLeases = Collections.newSetFromMap(new LinkedHashMap<LeaseInfo, Boolean>() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean removeEldestEntry(final Map.Entry<LeaseInfo, Boolean> eldest) {
            return size() > revokedLeasesToKeep;
        }
    });

    // private static final long maxTtlAllowed = TimeUnit.SECONDS.convert(7l, TimeUnit.DAYS);
    private final long maxTtlSecondsAllowed;
    private final long leaseAuditorIntervalSeconds;
    private LeaseAuditor leaseAuditor;

    MemoryLeaser(final long maxTtlDaysAllowed, final long leaseAuditorIntervalSeconds) {
        this.identity = UUID.randomUUID().toString();
        this.running = new AtomicBoolean(false);
        this.maxTtlSecondsAllowed = TimeUnit.SECONDS.convert(maxTtlDaysAllowed, TimeUnit.DAYS);
        this.leaseAuditorIntervalSeconds = leaseAuditorIntervalSeconds;
    }

    @Override
    public void start() throws LeaserException {
        if (running.compareAndSet(false, true)) {
            // cleanly handle resumption cases
            liveLeases.clear();
            expiredLeases.clear();
            revokedLeases.clear();
            leaseAuditor = new LeaseAuditor(leaseAuditorIntervalSeconds);
            leaseAuditor.start();
            logger.info("Started MemoryLeaser [{}]", identity);
        } else {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to start an already running leaser");
        }
    }

    @Override
    public void stop() throws LeaserException {
        if (running.compareAndSet(true, false)) {
            leaseAuditor.interrupt();
            logger.info("Stopped MemoryLeaser [{}]", identity);
        } else {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to stop an already stopped leaser");
        }
    }

    @Override
    public LeaseInfo acquireLease(final String ownerId, final String resourceId, final long ttlSeconds) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlSeconds);
        LeaseInfo leaseInfo = null;
        if (!liveLeases.containsKey(resourceId)) {
            leaseInfo = new LeaseInfo(ownerId, resourceId, ttlSeconds);
            liveLeases.put(resourceId, leaseInfo);
            logger.info("Acquired {}", leaseInfo);
        } else {
            throw new LeaserException(Code.LEASE_ALREADY_EXISTS, String.format("Lease already taken for resourceId:%s", resourceId));
        }
        return leaseInfo;
    }

    @Override
    public boolean revokeLease(final String ownerId, final String resourceId) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        boolean revoked = false;
        final LeaseInfo leaseInfo = liveLeases.get(resourceId);
        // check ownership
        if (leaseInfo != null && leaseInfo.getOwnerId().equals(ownerId) && leaseInfo.getResourceId().equals(resourceId)) {
            // check expiration
            if (expiredLeases.contains(leaseInfo)) {
                throw new LeaserException(Code.LEASE_ALREADY_EXPIRED,
                        String.format("Lease for ownerId:%s and resourceId:%s is already expired", ownerId, resourceId));
            }
            // now revoke
            {
                // leaseInfo.revoke();
                revokedLeases.add(leaseInfo);
                liveLeases.remove(resourceId);
                logger.info("Revoked {}", leaseInfo);
                revoked = true;
            }
        } else {
            throw new LeaserException(Code.LEASE_NOT_FOUND,
                    String.format("Lease for ownerId:%s and resourceId:%s can't be found", ownerId, resourceId));
        }
        return revoked;
    }

    @Override
    public LeaseInfo extendLease(final String ownerId, final String resourceId, final long ttlExtendBySeconds) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlExtendBySeconds);
        final LeaseInfo leaseInfo = liveLeases.get(resourceId);
        if (leaseInfo != null && leaseInfo.getOwnerId().equals(ownerId)) {
            long prevExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
            leaseInfo.extendTtlSeconds(ttlExtendBySeconds);
            long newExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
            logger.debug("Lease expiration seconds, prev:{}, new:{}", prevExpirationSeconds, newExpirationSeconds);
        }
        logger.info("Extended {} by {} seconds", leaseInfo, ttlExtendBySeconds);
        return leaseInfo;
    }

    @Override
    public LeaseInfo getLeaseInfo(final String ownerId, final String resourceId) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        LeaseInfo leaseInfo = liveLeases.get(resourceId);
        if (leaseInfo != null && ownerId.equals(leaseInfo.getOwnerId())) {
            logger.info("Found: {}", leaseInfo);
        } else {
            leaseInfo = null;
        }
        return leaseInfo;
    }

    private boolean validateTtlSeconds(final long ttlSeconds) throws LeaserException {
        if (ttlSeconds <= 0L || ttlSeconds > maxTtlSecondsAllowed) {
            throw new LeaserException(Code.INVALID_LEASE_TTL, String.format("Invalid lease ttl seconds:%d", ttlSeconds));
        }
        return true;
    }

    @Override
    public Set<LeaseInfo> getExpiredLeases() {
        return Collections.unmodifiableSet(expiredLeases);
    }

    @Override
    public Set<LeaseInfo> getRevokedLeases() {
        return Collections.unmodifiableSet(revokedLeases);
    }

    /**
     * A simple auditor daemon for watching leases.
     */
    private final class LeaseAuditor extends Thread {
        private final long runIntervalSeconds;

        private LeaseAuditor(final long runIntervalSeconds) {
            setDaemon(true);
            setName("lease-auditor");
            this.runIntervalSeconds = runIntervalSeconds;
            logger.info("Started LeaseAuditor");
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    logger.info("Auditing leases, live:{}, expired:{}", liveLeases.size(), expiredLeases.size());
                    for (final Map.Entry<String, LeaseInfo> leaseEntry : liveLeases.entrySet()) {
                        if (leaseEntry != null) {
                            final String resourceId = leaseEntry.getKey();
                            final LeaseInfo leaseInfo = leaseEntry.getValue();
                            if (Instant.now().isAfter(Instant.ofEpochSecond(leaseInfo.getExpirationEpochSeconds()))) {
                                expiredLeases.add(leaseInfo);
                                liveLeases.remove(resourceId);
                                logger.info("Expired {}", leaseInfo);
                            }
                        }
                    }
                    sleep(TimeUnit.MILLISECONDS.convert(runIntervalSeconds, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    break;
                }
            }
            logger.info("Stopped LeaseAuditor");
        }
    }

}
