package com.github.leaser.server;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.leaser.server.LeaserServerException.Code;

import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;

/**
 * Etcd based implementation of the Leaser
 */
public final class EtcdPersistentLeaser implements Leaser {
    private static final Logger logger = LogManager.getLogger(EtcdPersistentLeaser.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();

    private final AtomicBoolean running;
    private final AtomicBoolean ready;

    private final long maxTtlSecondsAllowed;
    private final long leaseAuditorIntervalSeconds;
    private LeaseAuditor leaseAuditor;
    private EtcdCluster cluster;
    private Client client;

    private final Map<Integer, String> table = new HashMap<>();

    private final Integer EXPIREDLEASES = 0;
    private final Integer LIVELEASES = 1;
    private final Integer REVOKEDLEASES = 2;

    private static Lock lockOperation = new ReentrantLock();

    EtcdPersistentLeaser(final long maxTtlDaysAllowed, final long leaseAuditorIntervalSeconds) {
        this.running = new AtomicBoolean(false);
        this.ready = new AtomicBoolean(false);
        this.maxTtlSecondsAllowed = TimeUnit.SECONDS.convert(maxTtlDaysAllowed, TimeUnit.DAYS);
        this.leaseAuditorIntervalSeconds = leaseAuditorIntervalSeconds;

        // reference to each table in etcd land
        table.put(0, new String("./leasersetcd/expiredLeases/"));
        table.put(1, new String("./leasersetcd/liveLeases/"));
        table.put(2, new String("./leasersetcd/revokedLeases/"));
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public void start() throws LeaserServerException {
        if (running.compareAndSet(false, true)) {
            try {
                cluster = EtcdClusterFactory.buildCluster(getClass().getSimpleName(), 1, false);
                cluster.start();
                client = Client.builder().endpoints(cluster.getClientEndpoints()).build();
            } catch (IllegalStateException excepIllegal) {
                try {
                    // try to attach to the local daemon
                    logger.info("etcd already started, listening on http://127.0.0.1:2379");
                    client = Client.builder().endpoints("http://127.0.0.1:2379").build();
                } catch (NullPointerException | IllegalArgumentException excepInvalid) {
                    throw new LeaserServerException(Code.LEASER_INVALID_ARG, "Invalid arguments to attach to etcd daemon");
                }
            }
            leaseAuditor = new LeaseAuditor(leaseAuditorIntervalSeconds);
            leaseAuditor.start();
            ready.set(true);
            logger.info("Started PersistentLeaser [{}]", getIdentity());
        } else {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to start an already running leaser");
        }
    }

    // For debuggin purposes - print entire table
    private void printLease(Integer tabId) {
        try {
            String prefix = table.get(tabId);
            ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
            GetOption getOption = GetOption.newBuilder().withPrefix(prefixByteSequence).build();
            List<KeyValue> values = client.getKVClient().get(prefixByteSequence, getOption).get().getKvs();
            logger.info("Table:" + tabId + " size: " + values.size());
            for (KeyValue kv : values) {
                final byte[] serializedLeaseInfo = kv.getValue().getBytes();
                LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                logger.info(leaseInfo);
            }
        } catch (InterruptedException excepInt) {
            // nothing to do
        } catch (ExecutionException excepExe) {
            logger.info("Exception " + excepExe.getCause());
            excepExe.printStackTrace();
        }
    }

    private List<KeyValue> getSetLeases(Integer tabId) {
        List<KeyValue> values = null;
        try {
            String prefix = table.get(tabId);
            ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
            GetOption getOption = GetOption.newBuilder().withPrefix(prefixByteSequence).build();
            values = client.getKVClient().get(prefixByteSequence, getOption).get().getKvs();
        } catch (InterruptedException excepInt) {
            // nothing to do
        } catch (ExecutionException excepExe) {
            logger.info("Exception " + excepExe.getCause());
            excepExe.printStackTrace();
        }
        return values;
    }

    private LeaseInfo getLease(String resourceId, Integer tabId) {
        LeaseInfo leaseInfo = null;
        try {
            String prefix = table.get(tabId) + resourceId;
            ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
            GetOption getOption = GetOption.newBuilder().withPrefix(prefixByteSequence).build();
            List<KeyValue> values = client.getKVClient().get(prefixByteSequence, getOption).get().getKvs();
            if (values.size() > 0) {
                KeyValue kv = values.get(0);
                final byte[] serializedLeaseInfo = kv.getValue().getBytes();
                leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
            }
        } catch (InterruptedException excepInt) {
            // nothing to do
        } catch (ExecutionException excepExe) {
            logger.info("Exception " + excepExe.getCause());
            excepExe.printStackTrace();
        }
        return leaseInfo;
    }

    private void cleanTable(Integer tabId) {
        String prefix = table.get(tabId);
        ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
        try {
            DeleteOption delOption = DeleteOption.newBuilder().withPrefix(prefixByteSequence).build();
            client.getKVClient().delete(ByteSequence.from(prefix, StandardCharsets.UTF_8), delOption).get();
        } catch (InterruptedException excepInt) {
            // nothing to do
        } catch (ExecutionException excepExe) {
            logger.info("Exception " + excepExe.getCause());
            excepExe.printStackTrace();
        }
    }

    private boolean remLease(String resourceId, Integer tabId) {
        boolean status = false;
        String prefix = table.get(tabId) + resourceId;
        ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
        try {
            DeleteOption delOption = DeleteOption.newBuilder().withPrefix(prefixByteSequence).build();
            client.getKVClient().delete(ByteSequence.from(prefix, StandardCharsets.UTF_8), delOption).get();
            status = true;
        } catch (InterruptedException excepInt) {
            // nothing to do
        } catch (ExecutionException excepExe) {
            logger.info("Exception " + excepExe.getCause());
            excepExe.printStackTrace();
        }
        return status;
    }

    private boolean addLease(LeaseInfo leaseInfo, Integer tabId) {
        boolean status = false;
        final byte[] serializedLease = LeaseInfo.serialize(leaseInfo);
        String prefix = table.get(tabId) + leaseInfo.getResourceId();
        ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
        ByteSequence leaseByteSequence = ByteSequence.from(serializedLease);
        try {
            client.getKVClient().put(ByteSequence.from(prefix, StandardCharsets.UTF_8), leaseByteSequence).get();
            status = true;
        } catch (InterruptedException excepInt) {
            // nothing to do
        } catch (ExecutionException excepExe) {
            logger.info("Exception " + excepExe.getCause());
            excepExe.printStackTrace();
        }
        return status;
    }

    @Override
    public LeaseInfo acquireLease(final String ownerId, final String resourceId, final long ttlSeconds) throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        validateTtlSeconds(ttlSeconds);
        final LeaseInfo leaseInfo = new LeaseInfo(ownerId, resourceId, ttlSeconds);
        boolean status = false;

        if (getLease(resourceId, LIVELEASES) != null) {
            throw new LeaserServerException(Code.LEASE_ALREADY_EXISTS, String.format("Lease already taken for resourceId:%s", resourceId));
        }
        status = addLease(leaseInfo, LIVELEASES);
        final byte[] serializedLease = LeaseInfo.serialize(leaseInfo);
        String prefix = table.get(LIVELEASES) + resourceId;
        ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
        ByteSequence leaseByteSequence = ByteSequence.from(serializedLease);
        try {
            client.getKVClient().put(ByteSequence.from(prefix, StandardCharsets.UTF_8), leaseByteSequence).get();
        } catch (InterruptedException excepInt) {
            return null;
        } catch (ExecutionException excepExe) {
            // logger.info("Exception " + excepExe.getCause());
            excepExe.printStackTrace();
            throw new LeaserServerException(Code.LEASE_PERSISTENCE_FAILURE, excepExe);
        }
        return leaseInfo;
    }

    @Override
    public boolean revokeLease(final String ownerId, final String resourceId) throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        boolean status = false;
        boolean revoked = false;
        try {
            lockOperation.lock();
            LeaseInfo leaseInfo = getLeaseInfo(ownerId, resourceId);
            if (leaseInfo != null) {
                status = addLease(leaseInfo, REVOKEDLEASES);
                if (!status)
                    logger.info("Problem revoking lease for resource " + resourceId);
                else {
                    status = remLease(resourceId, LIVELEASES);
                    logger.info("Revoked {}", leaseInfo);
                    revoked = true;
                }
            }
            // check if lease is expired, if yes, throw appropriate exception
            else {
                leaseInfo = getLease(resourceId, EXPIREDLEASES);
                if (leaseInfo != null) {
                    throw new LeaserServerException(Code.LEASE_ALREADY_EXPIRED,
                            String.format("Lease for ownerId:%s and resourceId:%s is expired", ownerId, resourceId));
                } else {
                    throw new LeaserServerException(Code.LEASE_NOT_FOUND,
                            String.format("Lease for ownerId:%s and resourceId:%s can't be found", ownerId, resourceId));
                }
            }
        } catch (LeaserServerException e) {
            throw e;
        } finally {
            lockOperation.unlock();
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
            lockOperation.lock();
            leaseInfo = getLease(resourceId, LIVELEASES);
            if (leaseInfo != null) {
                if (leaseInfo.getOwnerId().equals(ownerId)) {
                    final long prevExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
                    leaseInfo.extendTtlSeconds(ttlExtendBySeconds);
                    final long newExpirationSeconds = leaseInfo.getExpirationEpochSeconds();
                    boolean status = addLease(leaseInfo, LIVELEASES);
                    logger.debug("Lease expiration seconds, prev:{}, new:{}", prevExpirationSeconds, newExpirationSeconds);
                }
                logger.info("Extended {} by {} seconds", leaseInfo, ttlExtendBySeconds);
            }
        } finally {
            lockOperation.unlock();
        }
        return leaseInfo;
    }

    @Override
    public LeaseInfo getLeaseInfo(final String ownerId, final String resourceId) throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        LeaseInfo leaseInfo = getLease(resourceId, LIVELEASES);
        if (leaseInfo != null) {
            if (!leaseInfo.getOwnerId().equals(ownerId)) {
                leaseInfo = null;
            }
        }
        return leaseInfo;
    }

    @Override
    public Set<LeaseInfo> getExpiredLeases() throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        final Set<LeaseInfo> recentExpiredLeases = new LinkedHashSet<>();
        List<KeyValue> values = getSetLeases(EXPIREDLEASES);

        for (KeyValue val : values) {
            final byte[] serializedLeaseInfo = val.getValue().getBytes();
            if (serializedLeaseInfo != null) {
                final LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                recentExpiredLeases.add(leaseInfo);
            }
        }
        return recentExpiredLeases;
    }

    @Override
    public Set<LeaseInfo> getRevokedLeases() throws LeaserServerException {
        if (!isRunning()) {
            throw new LeaserServerException(Code.INVALID_LEASER_LCM, "Invalid attempt to operate an already stopped leaser");
        }
        final Set<LeaseInfo> recentRevokedLeases = new LinkedHashSet<>();
        List<KeyValue> values = getSetLeases(REVOKEDLEASES);
        if (values != null) {
            for (KeyValue val : values) {
                final byte[] serializedLeaseInfo = val.getValue().getBytes();
                if (serializedLeaseInfo != null) {
                    final LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                    recentRevokedLeases.add(leaseInfo);
                }
            }
        }
        return recentRevokedLeases;
    }

    @Override
    public void stop() throws LeaserServerException {
        if (running.compareAndSet(true, false)) {
            try {
                ready.set(false);
                leaseAuditor.interrupt();
                cleanTable(EXPIREDLEASES);
                cleanTable(LIVELEASES);
                cleanTable(REVOKEDLEASES);
                cluster.close();
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
                    logger.info("Auditing leases");
                    List<KeyValue> values = getSetLeases(LIVELEASES);
                    if (values != null) {
                        for (final KeyValue kv : values) {
                            final byte[] serializedResourceId = kv.getKey().getBytes();
                            final byte[] serializedLeaseInfo = kv.getValue().getBytes();
                            if (serializedResourceId != null && serializedLeaseInfo != null) {
                                final LeaseInfo leaseInfo = LeaseInfo.deserialize(serializedLeaseInfo);
                                if (Instant.now().isAfter(Instant.ofEpochSecond(leaseInfo.getExpirationEpochSeconds()))) {
                                    try {
                                        lockOperation.lock();
                                        // check again if lease is in the live leases table
                                        if (getLease(leaseInfo.getResourceId(), LIVELEASES) != null) {
                                            remLease(leaseInfo.getResourceId(), LIVELEASES);
                                            addLease(leaseInfo, EXPIREDLEASES);
                                            logger.info("Expired {}", leaseInfo);
                                        }
                                    } finally {
                                        lockOperation.unlock();
                                    }
                                }
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
