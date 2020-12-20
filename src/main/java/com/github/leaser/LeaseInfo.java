package com.github.leaser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

/**
 * This object represents the metadata associated with a lease on a Resource.
 */
public final class LeaseInfo implements Comparable<LeaseInfo> {
    // immutables
    private String leaseId; // 1
    private Instant created; // 2
    private String ownerId; // 3
    private String resourceId; // 4

    private volatile long ttlSeconds; // 5
    private volatile long revision; // 6
    private volatile Instant lastUpdated; // 7
    private volatile Instant expirationEpochSeconds; // 8

    public LeaseInfo(final String ownerId, final String resourceId, final long ttlSeconds) {
        this.leaseId = UUID.randomUUID().toString();
        this.ownerId = ownerId;
        this.resourceId = resourceId;
        this.ttlSeconds = ttlSeconds;
        this.created = Instant.now();
        this.expirationEpochSeconds = created.plusSeconds(ttlSeconds);
        this.revision = 1;
    }

    @Override
    public int compareTo(final LeaseInfo other) {
        // TODO: avoid blowing up via npe
        int comparison = 0;
        comparison = this.leaseId.compareTo(other.leaseId);
        if (comparison != 0) {
            return comparison;
        }
        comparison = this.created.compareTo(other.created);
        if (comparison != 0) {
            return comparison;
        }
        comparison = this.ownerId.compareTo(other.ownerId);
        if (comparison != 0) {
            return comparison;
        }
        comparison = this.resourceId.compareTo(other.resourceId);
        if (comparison != 0) {
            return comparison;
        }
        comparison = Long.compare(this.ttlSeconds, other.ttlSeconds);
        if (comparison != 0) {
            return comparison;
        }
        comparison = Long.compare(this.revision, other.revision);
        if (comparison != 0) {
            return comparison;
        }
        comparison = this.lastUpdated.compareTo(other.lastUpdated);
        if (comparison != 0) {
            return comparison;
        }
        comparison = this.expirationEpochSeconds.compareTo(other.expirationEpochSeconds);
        return comparison;
    }

    // Serialize the provided LeaseInfo into a byte array
    public static byte[] serialize(final LeaseInfo leaseInfo) {
        // (int, byte[]), long, (int, byte[]), (int, byte[]), long, long, long, long
        final ByteBuffer buffer = ByteBuffer.allocate(2048);
        // 1. leaseId
        if (leaseInfo.getLeaseId() != null) {
            final byte[] leaseIdBytes = leaseInfo.getLeaseId().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(leaseIdBytes.length);
            buffer.put(leaseIdBytes);
        } else {
            buffer.putInt(0);
            buffer.put(new byte[0]);
        }
        // 2. created
        if (leaseInfo.getCreated() != null) {
            buffer.putLong(leaseInfo.getCreated().toInstant().getEpochSecond());
        } else {
            buffer.putLong(Long.MIN_VALUE);
        }
        // 3. ownerId
        if (leaseInfo.getOwnerId() != null) {
            final byte[] ownerIdBytes = leaseInfo.getOwnerId().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(ownerIdBytes.length);
            buffer.put(ownerIdBytes);
        } else {
            buffer.putInt(0);
            buffer.put(new byte[0]);
        }
        // 4. resourceId
        if (leaseInfo.getResourceId() != null) {
            final byte[] resourceIdBytes = leaseInfo.getResourceId().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(resourceIdBytes.length);
            buffer.put(resourceIdBytes);
        } else {
            buffer.putInt(0);
            buffer.put(new byte[0]);
        }
        buffer.putLong(leaseInfo.getTtlSeconds()); // 5. ttlSeconds
        buffer.putLong(leaseInfo.getRevision()); // 6. revision
        // 7. lastUpdated
        if (leaseInfo.getLastUpdate() != null) {
            buffer.putLong(leaseInfo.getLastUpdate().toInstant().getEpochSecond());
        } else {
            buffer.putLong(Long.MIN_VALUE);
        }
        buffer.putLong(leaseInfo.getExpirationEpochSeconds()); // 8. expirationEpochSeconds
        buffer.rewind();
        return buffer.array();
    }

    // Deserialize the provided byte array into it LeaseInfo
    public static LeaseInfo deserialize(final byte[] leaseInfoBytes) {
        // (int, byte[]), long, (int, byte[]), (int, byte[]), long, long, long, long
        final ByteBuffer buffer = ByteBuffer.wrap(leaseInfoBytes);
        // 1. leaseId
        String leaseId = null;
        final int leaseIdLength = buffer.getInt();
        if (leaseIdLength != 0) {
            final byte[] leaseIdBytes = new byte[leaseIdLength];
            buffer.get(leaseIdBytes);
            leaseId = new String(leaseIdBytes, StandardCharsets.UTF_8);
        }
        // 2. created
        Instant created = null;
        final long createdSeconds = buffer.getLong();
        if (createdSeconds != Long.MIN_VALUE) {
            created = Instant.ofEpochSecond(createdSeconds);
        }
        // 3. ownerId
        String ownerId = null;
        final int ownerIdLength = buffer.getInt();
        if (ownerIdLength != 0) {
            final byte[] ownerIdBytes = new byte[ownerIdLength];
            buffer.get(ownerIdBytes);
            ownerId = new String(ownerIdBytes, StandardCharsets.UTF_8);
        }
        // 4. resourceId
        String resourceId = null;
        final int resourceIdLength = buffer.getInt();
        if (resourceIdLength != 0) {
            final byte[] resourceIdBytes = new byte[resourceIdLength];
            buffer.get(resourceIdBytes);
            resourceId = new String(resourceIdBytes, StandardCharsets.UTF_8);
        }
        final long ttlSeconds = buffer.getLong(); // 5. ttlSeconds
        final long revision = buffer.getLong(); // 6. revision
        // 7. lastUpdated
        Instant lastUpdated = null;
        final long lastUpdatedSeconds = buffer.getLong();
        if (lastUpdatedSeconds != Long.MIN_VALUE) {
            lastUpdated = Instant.ofEpochSecond(lastUpdatedSeconds);
        }
        // 8. expirationEpochSeconds
        Instant expirationEpochSeconds = null;
        final long expirationEpoch = buffer.getLong();
        if (expirationEpoch != Long.MIN_VALUE) {
            expirationEpochSeconds = Instant.ofEpochSecond(expirationEpoch);
        }
        // now curate the lease
        final LeaseInfo leaseInfo = new LeaseInfo(ownerId, resourceId, ttlSeconds);
        leaseInfo.leaseId = leaseId;
        leaseInfo.created = created;
        leaseInfo.revision = revision;
        leaseInfo.lastUpdated = lastUpdated;
        leaseInfo.expirationEpochSeconds = expirationEpochSeconds;
        return leaseInfo;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public Date getCreated() {
        return created != null ? Date.from(created) : null;
    }

    public Date getLastUpdate() {
        return lastUpdated != null ? Date.from(lastUpdated) : null;
    }

    public long getRevision() {
        return revision;
    }

    public long getTtlSeconds() {
        return ttlSeconds;
    }

    public long getExpirationEpochSeconds() {
        return expirationEpochSeconds.getEpochSecond();
    }

    public synchronized void extendTtlSeconds(final long ttlExtendBySeconds) {
        this.revision++;
        this.ttlSeconds = ttlExtendBySeconds;
        this.expirationEpochSeconds = expirationEpochSeconds.plusSeconds(ttlExtendBySeconds);
        this.lastUpdated = Instant.now();
    }

    // consider touching the lease to place a revocation mark
    // public void revoke() {
    // this.revoked = Instant.now();
    // }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((leaseId == null) ? 0 : leaseId.hashCode());
        result = prime * result + ((ownerId == null) ? 0 : ownerId.hashCode());
        result = prime * result + ((resourceId == null) ? 0 : resourceId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LeaseInfo)) {
            return false;
        }
        LeaseInfo other = (LeaseInfo) obj;
        if (leaseId == null) {
            if (other.leaseId != null) {
                return false;
            }
        } else if (!leaseId.equals(other.leaseId)) {
            return false;
        }
        if (ownerId == null) {
            if (other.ownerId != null) {
                return false;
            }
        } else if (!ownerId.equals(other.ownerId)) {
            return false;
        }
        if (resourceId == null) {
            if (other.resourceId != null) {
                return false;
            }
        } else if (!resourceId.equals(other.resourceId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Lease [leaseId:" + leaseId + ", created:" + created + ", ownerId:" + ownerId + ", resourceId:" + resourceId + ", ttlSeconds:"
                + ttlSeconds + ", revision:" + revision + ", lastUpdated:" + lastUpdated + ", expirationEpochSeconds:" + expirationEpochSeconds + "]";
    }

}
