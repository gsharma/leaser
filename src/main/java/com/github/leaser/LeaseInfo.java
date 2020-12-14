package com.github.leaser;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

/**
 * This object represents the metadata associated with a lease on a Resource.
 */
public final class LeaseInfo {
    // immutable
    private final String leaseId;
    private final Instant created;
    private final String ownerId;
    private final String resourceId;

    private volatile long ttlSeconds;
    private volatile long revision;
    private volatile Instant lastUpdated;
    private volatile Instant expirationEpochSeconds;

    public LeaseInfo(final String ownerId, final String resourceId, final long ttlSeconds) {
        this.leaseId = UUID.randomUUID().toString();
        this.ownerId = ownerId;
        this.resourceId = resourceId;
        this.ttlSeconds = ttlSeconds;
        this.created = Instant.now();
        this.expirationEpochSeconds = created.plusSeconds(ttlSeconds);
        this.revision = 1;
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
