package com.github.leaser;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.github.leaser.LeaserException.Code;

/**
 * This object represents the metadata associated with a lease on a Resource.
 */
public final class LeaseInfo {
    // immutable
    private final String leaseId;
    private final Instant created;
    private final String resourceId;

    private static final long maxTtlAllowed = TimeUnit.SECONDS.convert(7l, TimeUnit.DAYS);

    private volatile long ttlSeconds;
    private volatile long revision;
    private volatile Instant lastUpdated;
    private volatile Instant expirationEpochSeconds;

    public LeaseInfo(final String resourceId, final long ttlSeconds) throws LeaserException {
        validateTtlSeconds(ttlSeconds);
        this.leaseId = UUID.randomUUID().toString();
        this.resourceId = resourceId;
        this.ttlSeconds = ttlSeconds;
        this.created = Instant.now();
        this.expirationEpochSeconds = created.plusSeconds(ttlSeconds);
        this.revision = 1;
    }

    public String getLeaseId() {
        return leaseId;
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

    private static boolean validateTtlSeconds(final long ttlSeconds) throws LeaserException {
        if (ttlSeconds <= 0L || ttlSeconds > maxTtlAllowed) {
            throw new LeaserException(Code.INVALID_LEASE_TTL, String.format("Invalid lease ttl seconds:%d", ttlSeconds));
        }
        return true;
    }

    public synchronized void extendTtlSeconds(final long ttlExtendBySeconds) throws LeaserException {
        validateTtlSeconds(ttlExtendBySeconds);
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
        return "LeaseInfo [leaseId=" + leaseId + ", created=" + created + ", resourceId=" + resourceId + ", ttlSeconds=" + ttlSeconds + ", revision="
                + revision + ", lastUpdated=" + lastUpdated + ", expirationEpochSeconds=" + expirationEpochSeconds + "]";
    }

}
