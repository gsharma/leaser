package com.github.leaser;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.github.leaser.LeaserServerException.Code;

/**
 * A rocksdb based implementation of the Leaser
 */
public final class RocksdbPersistentLeaser implements Leaser {
    private static final Logger logger = LogManager.getLogger(RocksdbPersistentLeaser.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();

    private final AtomicBoolean running;
    private final AtomicBoolean ready;

    private final long maxTtlSecondsAllowed;
    private final long leaseAuditorIntervalSeconds;
    private LeaseAuditor leaseAuditor;

    private File storeDirectory;
    private RocksDB dataStore;

    // private ColumnFamilyHandle defaultCF;
    private ColumnFamilyHandle liveLeases;
    private ColumnFamilyHandle expiredLeases;
    private ColumnFamilyHandle revokedLeases;

    RocksdbPersistentLeaser(final long maxTtlDaysAllowed, final long leaseAuditorIntervalSeconds) {
        this.running = new AtomicBoolean(false);
        this.ready = new AtomicBoolean(false);
        this.maxTtlSecondsAllowed = TimeUnit.SECONDS.convert(maxTtlDaysAllowed, TimeUnit.DAYS);
        this.leaseAuditorIntervalSeconds = leaseAuditorIntervalSeconds;
    }

    @Override
    public void start() throws LeaserServerException {
        if (running.compareAndSet(false, true)) {
            // cleanly handle resumption cases
            try {
                final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
                final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(4);
                columnFamilyDescriptors.add(0, new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
                columnFamilyDescriptors.add(1, new ColumnFamilyDescriptor("liveLeases".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
                columnFamilyDescriptors.add(2, new ColumnFamilyDescriptor("expiredLeases".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
                columnFamilyDescriptors.add(3, new ColumnFamilyDescriptor("revokedLeases".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));

                final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

                RocksDB.loadLibrary();
                final DBOptions options = new DBOptions();
                options.setCreateIfMissing(true);
                options.setCreateMissingColumnFamilies(true);
                storeDirectory = new File("./leasedb", "leaser");
                Files.createDirectories(storeDirectory.getParentFile().toPath());
                Files.createDirectories(storeDirectory.getAbsoluteFile().toPath());
                dataStore = RocksDB.open(options, storeDirectory.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);

                // defaultCF = columnFamilyHandles.get(0);
                liveLeases = columnFamilyHandles.get(1);
                expiredLeases = columnFamilyHandles.get(2);
                revokedLeases = columnFamilyHandles.get(3);
            } catch (Exception initProblem) {
                throw new LeaserServerException(Code.LEASER_INIT_FAILURE, initProblem);
            }
            leaseAuditor = new LeaseAuditor(leaseAuditorIntervalSeconds);
            leaseAuditor.start();
            ready.set(true);
            logger.info("Started PersistentLeaser [{}]", getIdentity());
        } else {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to start an already running leaser");
        }
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public LeaseInfo acquireLease(final String ownerId, final String resourceId, final long ttlSeconds) throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlSeconds);
        final LeaseInfo leaseInfo = new LeaseInfo(ownerId, resourceId, ttlSeconds);
        try {
            final byte[] serializedResourceId = resourceId.getBytes(StandardCharsets.UTF_8);
            final byte[] serializedLease = LeaseInfo.serialize(leaseInfo);
            if (dataStore.get(liveLeases, serializedResourceId) != null) {
                throw new LeaserServerException(Code.LEASE_ALREADY_EXISTS, String.format("Lease already taken for resourceId:%s", resourceId));
            }
            dataStore.put(liveLeases, serializedResourceId, serializedLease);
            logger.info("Acquired {}", leaseInfo);
        } catch (RocksDBException persistenceIssue) {
            throw new LeaserServerException(Code.LEASE_PERSISTENCE_FAILURE, persistenceIssue);
        }
        return leaseInfo;
    }

    @Override
    public boolean revokeLease(final String ownerId, final String resourceId) throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        boolean revoked = false;
        try {
            final byte[] serializedResourceId = resourceId.getBytes(StandardCharsets.UTF_8);
            final byte[] serializedLeaseInfo = dataStore.get(liveLeases, serializedResourceId);
            if (serializedLeaseInfo != null) {
                LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                // check ownership
                if (leaseInfo.getOwnerId().equals(ownerId) && leaseInfo.getResourceId().equals(resourceId)) {
                    // TODO: check expiration
                    if (dataStore.get(expiredLeases, serializedResourceId) != null) {
                        throw new LeaserServerException(Code.LEASE_ALREADY_EXPIRED,
                                String.format("Lease for ownerId:%s and resourceId:%s is already expired", ownerId, resourceId));
                    }
                    // TODO: better to do this atomically via a db txn
                    {
                        dataStore.put(revokedLeases, serializedResourceId, serializedLeaseInfo);
                        dataStore.delete(liveLeases, serializedResourceId);
                        logger.info("Revoked {}", leaseInfo);
                        revoked = true;
                    }
                } else {
                    throw new LeaserServerException(Code.LEASE_NOT_FOUND,
                            String.format("Lease for ownerId:%s and resourceId:%s can't be found", ownerId, resourceId));
                }
            }
        } catch (RocksDBException persistenceIssue) {
            throw new LeaserServerException(Code.LEASE_PERSISTENCE_FAILURE, persistenceIssue);
        }
        return revoked;
    }

    @Override
    public LeaseInfo extendLease(final String ownerId, final String resourceId, final long ttlExtendBySeconds) throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlExtendBySeconds);
        LeaseInfo leaseInfo = null;
        try {
            final byte[] serializedResourceId = resourceId.getBytes(StandardCharsets.UTF_8);
            final byte[] serializedLeaseInfo = dataStore.get(liveLeases, serializedResourceId);
            if (serializedLeaseInfo != null) {
                leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                if (leaseInfo.getOwnerId().equals(ownerId)) {
                    final long prevExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
                    leaseInfo.extendTtlSeconds(ttlExtendBySeconds);
                    final long newExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
                    dataStore.put(liveLeases, serializedResourceId, LeaseInfo.serialize(leaseInfo));
                    logger.debug("Lease expiration seconds, prev:{}, new:{}", prevExpirationSeconds, newExpirationSeconds);
                }
                logger.info("Extended {} by {} seconds", leaseInfo, ttlExtendBySeconds);
            }
        } catch (RocksDBException persistenceIssue) {
            throw new LeaserServerException(Code.LEASE_PERSISTENCE_FAILURE, persistenceIssue);
        }
        return leaseInfo;
    }

    @Override
    public LeaseInfo getLeaseInfo(final String ownerId, final String resourceId) throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        LeaseInfo leaseInfo = null;
        try {
            final byte[] serializedResourceId = resourceId.getBytes(StandardCharsets.UTF_8);
            final byte[] serializedLeaseInfo = dataStore.get(liveLeases, serializedResourceId);
            if (serializedLeaseInfo != null) {
                leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                if (!leaseInfo.getOwnerId().equals(ownerId)) {
                    leaseInfo = null;
                }
            }
            logger.info("Found {}", leaseInfo);
        } catch (RocksDBException persistenceIssue) {
            throw new LeaserServerException(Code.LEASE_PERSISTENCE_FAILURE, persistenceIssue);
        }
        return leaseInfo;
    }

    @Override
    public Set<LeaseInfo> getExpiredLeases() throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        final Set<LeaseInfo> recentExpiredLeases = new LinkedHashSet<>();
        final RocksIterator expiredLeasesIter = dataStore.newIterator(expiredLeases);
        for (expiredLeasesIter.seekToFirst(); expiredLeasesIter.isValid(); expiredLeasesIter.next()) {
            final byte[] serializedLeaseInfo = expiredLeasesIter.value();
            if (serializedLeaseInfo != null) {
                final LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                recentExpiredLeases.add(leaseInfo);
            }
        }
        logger.info("Found {} expired leases", recentExpiredLeases.size());
        return recentExpiredLeases;
    }

    @Override
    public Set<LeaseInfo> getRevokedLeases() throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        final Set<LeaseInfo> recentRevokedLeases = new LinkedHashSet<>();
        final RocksIterator revokedLeasesIter = dataStore.newIterator(revokedLeases);
        for (revokedLeasesIter.seekToFirst(); revokedLeasesIter.isValid(); revokedLeasesIter.next()) {
            final byte[] serializedLeaseInfo = revokedLeasesIter.value();
            if (serializedLeaseInfo != null) {
                final LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                recentRevokedLeases.add(leaseInfo);
            }
        }
        logger.info("Found {} revoked leases", recentRevokedLeases.size());
        return recentRevokedLeases;
    }

    @Override
    public void stop() throws LeaserServerException {
        if (running.compareAndSet(true, false)) {
            try {
                ready.set(false);
                leaseAuditor.interrupt();
                // dataStore.dropColumnFamily(defaultCF);
                dataStore.dropColumnFamily(liveLeases);
                dataStore.dropColumnFamily(expiredLeases);
                dataStore.dropColumnFamily(revokedLeases);
                dataStore.close();
                Files.walk(storeDirectory.toPath())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                logger.info("Stopped PersistentLeaser [{}]", getIdentity());
            } catch (Exception tiniProblem) {
                throw new LeaserServerException(Code.LEASER_TINI_FAILURE, tiniProblem);
            }
        } else {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to stop an already stopped leaser");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }

    private boolean validateTtlSeconds(final long ttlSeconds) throws LeaserServerException {
        if (ttlSeconds <= 0L || ttlSeconds > maxTtlSecondsAllowed) {
            throw new LeaserServerException(Code.INVALID_LEASE_TTL, String.format("Invalid lease ttl seconds:%d", ttlSeconds));
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
            logger.info("Started LeaseAuditor [{}]", getIdentity());
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    // logger.info("Auditing leases, live:{}, expired:{}", liveLeases.size(), expiredLeases.size());
                    logger.info("Auditing leases");
                    try {
                        final RocksIterator liveLeasesIter = dataStore.newIterator(liveLeases);
                        for (liveLeasesIter.seekToFirst(); liveLeasesIter.isValid(); liveLeasesIter.next()) {
                            final byte[] serializedResourceId = liveLeasesIter.key();
                            final byte[] serializedLeaseInfo = liveLeasesIter.value();
                            if (serializedResourceId != null && serializedLeaseInfo != null) {
                                // final String resourceId = new String(serializedResourceId, StandardCharsets.UTF_8);
                                final LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                                if (Instant.now().isAfter(Instant.ofEpochSecond(leaseInfo.getExpirationEpochSeconds()))) {
                                    // TODO: better to do this atomically via a db txn
                                    dataStore.put(expiredLeases, serializedResourceId, serializedLeaseInfo);
                                    dataStore.delete(liveLeases, serializedResourceId);
                                    logger.info("Expired {}", leaseInfo);
                                }
                            }
                        }
                    } catch (RocksDBException persistenceIssue) {
                        logger.error("Encountered rocksdb persistence issue", persistenceIssue);
                    }
                    sleep(TimeUnit.MILLISECONDS.convert(runIntervalSeconds, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    break;
                }
            }
            logger.info("Stopped LeaseAuditor [{}]", getIdentity());
        }
    }

}
