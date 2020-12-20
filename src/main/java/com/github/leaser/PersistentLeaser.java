package com.github.leaser;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.github.leaser.LeaserException.Code;

/**
 * A rocksdb based implementation of the Leaser
 */
public final class PersistentLeaser implements Leaser {
    private static final Logger logger = LogManager.getLogger(PersistentLeaser.class.getSimpleName());

    private final String identity;
    private final AtomicBoolean running;

    private final long maxTtlSecondsAllowed;
    private final long leaseAuditorIntervalSeconds;
    private LeaseAuditor leaseAuditor;

    private File storeDirectory;
    private RocksDB dataStore;

    PersistentLeaser(final long maxTtlDaysAllowed, final long leaseAuditorIntervalSeconds) {
        this.identity = UUID.randomUUID().toString();
        this.running = new AtomicBoolean(false);
        this.maxTtlSecondsAllowed = TimeUnit.SECONDS.convert(maxTtlDaysAllowed, TimeUnit.DAYS);
        this.leaseAuditorIntervalSeconds = leaseAuditorIntervalSeconds;
    }

    @Override
    public void start() throws LeaserException {
        if (running.compareAndSet(false, true)) {
            // cleanly handle resumption cases
            RocksDB.loadLibrary();
            final Options options = new Options();
            options.setCreateIfMissing(true);
            options.setCreateMissingColumnFamilies(true);
            storeDirectory = new File("./leasedb", "leaser");
            try {
                Files.createDirectories(storeDirectory.getParentFile().toPath());
                Files.createDirectories(storeDirectory.getAbsoluteFile().toPath());
                dataStore = RocksDB.open(options, storeDirectory.getAbsolutePath());
            } catch (Exception initProblem) {
                throw new LeaserException(Code.LEASER_INIT_FAILURE, initProblem);
            }
            leaseAuditor = new LeaseAuditor(leaseAuditorIntervalSeconds);
            leaseAuditor.start();
            logger.info("Started PersistentLeaser [{}]", identity);
        } else {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to start an already running leaser");
        }
    }

    @Override
    public LeaseInfo acquireLease(String ownerId, String resourceId, long ttlSeconds) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlSeconds);
        final LeaseInfo leaseInfo = new LeaseInfo(ownerId, resourceId, ttlSeconds);
        try {
            final byte[] serializedResourceId = resourceId.getBytes(StandardCharsets.UTF_8);
            final byte[] serializedLease = LeaseInfo.serialize(leaseInfo);
            if (dataStore.get(serializedResourceId) != null) {
                throw new LeaserException(Code.LEASE_ALREADY_EXISTS, String.format("Lease already taken for resourceId:%s", resourceId));
            }
            dataStore.put(serializedResourceId, serializedLease);
            logger.info("Acquired {}", leaseInfo);
        } catch (RocksDBException persistenceIssue) {
            throw new LeaserException(Code.LEASE_PERSISTENCE_FAILURE, persistenceIssue);
        }
        return leaseInfo;
    }

    @Override
    public boolean revokeLease(String ownerId, String resourceId) throws LeaserException {
        if (!running.get()) {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        boolean revoked = false;
        try {
            final byte[] serializedResourceId = resourceId.getBytes(StandardCharsets.UTF_8);
            final byte[] serializedLeaseInfo = dataStore.get(serializedResourceId);
            if (serializedLeaseInfo != null) {
                LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                // check ownership
                if (leaseInfo.getOwnerId().equals(ownerId) && leaseInfo.getResourceId().equals(resourceId)) {
                    // TODO: check expiration
                    {
                        dataStore.delete(serializedResourceId);
                        logger.info("Revoked {}", leaseInfo);
                        revoked = true;
                    }
                } else {
                    throw new LeaserException(Code.LEASE_NOT_FOUND,
                            String.format("Lease for ownerId:%s and resourceId:%s can't be found", ownerId, resourceId));
                }
            }
        } catch (RocksDBException persistenceIssue) {
            throw new LeaserException(Code.LEASE_PERSISTENCE_FAILURE, persistenceIssue);
        }
        return revoked;
    }

    @Override
    public LeaseInfo extendLease(String ownerId, String resourceId, long ttlExtendBySeconds) throws LeaserException {
        // TODO
        return null;
    }

    @Override
    public LeaseInfo getLeaseInfo(String ownerId, String resourceId) throws LeaserException {
        LeaseInfo leaseInfo = null;
        try {
            final byte[] serializedResourceId = resourceId.getBytes(StandardCharsets.UTF_8);
            final byte[] serializedLeaseInfo = dataStore.get(serializedResourceId);
            if (serializedLeaseInfo != null) {
                leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                if (!leaseInfo.getOwnerId().equals(ownerId)) {
                    leaseInfo = null;
                }
            }
            logger.info("Found {}", leaseInfo);
        } catch (RocksDBException persistenceIssue) {
            throw new LeaserException(Code.LEASE_PERSISTENCE_FAILURE, persistenceIssue);
        }
        return leaseInfo;
    }

    @Override
    public void stop() throws LeaserException {
        if (running.compareAndSet(true, false)) {
            leaseAuditor.interrupt();
            dataStore.close();
            logger.info("Stopped PersistentLeaser [{}]", identity);
        } else {
            throw new LeaserException(Code.INVALID_LEASER_LCM, "Invalid attempt to stop an already stopped leaser");
        }
    }

    private boolean validateTtlSeconds(final long ttlSeconds) throws LeaserException {
        if (ttlSeconds <= 0L || ttlSeconds > maxTtlSecondsAllowed) {
            throw new LeaserException(Code.INVALID_LEASE_TTL, String.format("Invalid lease ttl seconds:%d", ttlSeconds));
        }
        return true;
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
                    // TODO
                    // logger.info("Auditing leases, live:{}, expired:{}", liveLeases.size(), expiredLeases.size());
                    sleep(TimeUnit.MILLISECONDS.convert(runIntervalSeconds, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    break;
                }
            }
            logger.info("Stopped LeaseAuditor");
        }
    }
}
