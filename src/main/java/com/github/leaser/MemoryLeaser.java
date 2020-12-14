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
    public LeaseInfo acquireLease(final String resourceId, final long ttlSeconds) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlSeconds);
        LeaseInfo leaseInfo = null;
        if (!liveLeases.containsKey(resourceId)) {
            leaseInfo = new LeaseInfo(resourceId, ttlSeconds);
            liveLeases.put(resourceId, leaseInfo);
            logger.info("Acquired {}", leaseInfo);
        } else {
            throw new LeaserException(Code.LEASE_ALREADY_EXISTS, String.format("Lease already taken for resourceId:%s", resourceId));
        }
        return leaseInfo;
    }

    @Override
    public boolean revokeLease(final String resourceId) throws LeaserException {
        // TODO: Denise to fill out & add tests
        return false;
    }

    @Override
    public LeaseInfo extendLease(final String resourceId, final long ttlExtendBySeconds) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlExtendBySeconds);
        final LeaseInfo leaseInfo = liveLeases.get(resourceId);
        if (leaseInfo != null) {
            long prevExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
            leaseInfo.extendTtlSeconds(ttlExtendBySeconds);
            long newExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
            logger.debug("Lease expiration seconds, prev:{}, new:{}", prevExpirationSeconds, newExpirationSeconds);
        }
        logger.info("Extended {} by {} seconds", leaseInfo, ttlExtendBySeconds);
        return leaseInfo;
    }

    @Override
    public LeaseInfo getLeaseInfo(final String resourceId) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        final LeaseInfo leaseInfo = liveLeases.get(resourceId);
        logger.info("For resourceId:{}, found: {}", resourceId, leaseInfo);
        return leaseInfo;
    }

    private boolean validateTtlSeconds(final long ttlSeconds) throws LeaserException {
        if (ttlSeconds <= 0L || ttlSeconds > maxTtlSecondsAllowed) {
            throw new LeaserException(Code.INVALID_LEASE_TTL, String.format("Invalid lease ttl seconds:%d", ttlSeconds));
        }
        return true;
    }

    // only for testing
    Set<LeaseInfo> getExpiredLeases() {
        return Collections.synchronizedSet(expiredLeases);
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
                    logger.info("Auditing live leases");
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
