package com.github.leaser;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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

    private final AtomicBoolean running = new AtomicBoolean();
    private final ConcurrentMap<String, LeaseInfo> liveLeases = new ConcurrentHashMap<>();

    private static final int expiredLeasesToKeep = 25;
    private final Set<LeaseInfo> expiredLeases = Collections.newSetFromMap(new LinkedHashMap<LeaseInfo, Boolean>() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean removeEldestEntry(final Map.Entry<LeaseInfo, Boolean> eldest) {
            return size() > expiredLeasesToKeep;
        }
    });

    private final LeaseAuditor leaseAuditor = new LeaseAuditor(1L);

    @Override
    public void start() throws LeaserException {
        if (running.compareAndSet(false, true)) {
            // handle resumption cases
            liveLeases.clear();
            expiredLeases.clear();
            leaseAuditor.start();
            logger.info("Started MemoryLeaser");
        } else {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to start an already running leaser");
        }
    }

    @Override
    public void stop() throws LeaserException {
        if (running.compareAndSet(true, false)) {
            leaseAuditor.interrupt();
            logger.info("Stopped MemoryLeaser");
        } else {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to stop an already stopped leaser");
        }
    }

    @Override
    public LeaseInfo acquireLease(final Resource resource, final long ttlSeconds) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        LeaseInfo leaseInfo = new LeaseInfo(resource.getId(), ttlSeconds);
        liveLeases.putIfAbsent(leaseInfo.getLeaseId(), leaseInfo);
        logger.info("Acquired {}", leaseInfo);
        return leaseInfo;
    }

    @Override
    public boolean revokeLease(final String leaseId) throws LeaserException {
        // TODO: Denise to fill out & add tests
        return false;
    }

    @Override
    public LeaseInfo extendLease(final String leaseId, final long ttlExtendBySeconds) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        final LeaseInfo leaseInfo = liveLeases.get(leaseId);
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
    public LeaseInfo getLeaseInfo(final String leaseId) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        final LeaseInfo leaseInfo = liveLeases.get(leaseId);
        logger.info("Found {}", leaseInfo);
        return leaseInfo;
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
                            final String leaseId = leaseEntry.getKey();
                            final LeaseInfo leaseInfo = leaseEntry.getValue();
                            if (Instant.now().isAfter(Instant.ofEpochSecond(leaseInfo.getExpirationEpochSeconds()))) {
                                expiredLeases.add(leaseInfo);
                                liveLeases.remove(leaseId);
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
