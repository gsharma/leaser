# Leaser
A leaser is responsible for helping with lease management for resources. It can operate in an in-memory or a persistent configuration and can efficiently help manage leases for many resources without using much memory or compute.

## Leaser Modes
Leaser can presently operate in 1 of 3 operational modes:
1. In-memory leaser
2. RocksDb-backed leaser
3. Etcd-backed leaser

## Leaser Client API Reference
```java
AcquireLeaseResponse acquireLease(final AcquireLeaseRequest request) throws LeaserClientException;

RevokeLeaseResponse revokeLease(final RevokeLeaseRequest request) throws LeaserClientException;

ExtendLeaseResponse extendLease(final ExtendLeaseRequest request) throws LeaserClientException;

GetLeaseInfoResponse getLeaseInfo(final GetLeaseInfoRequest request) throws LeaserClientException;

GetExpiredLeasesResponse getExpiredLeases(final GetExpiredLeasesRequest request) throws LeaserClientException;

GetRevokedLeasesResponse getRevokedLeases(final GetRevokedLeasesRequest request) throws LeaserClientException;

static LeaserClient getClient(final String serverHost, final int serverPort);
```

## Leaser API Reference
```java
// Start the Leaser.
void start() throws LeaserException;

// Acquire a lease on the given resourceId to be held by ownerId for the duration of ttlSeconds. Unless the lease is extended via
// extendLease(), this lease expires ttlSeconds after creation.
LeaseInfo acquireLease(final String ownerId, final String resourceId, final long ttlSeconds) throws LeaserException;

// Revoke a lease held by ownerId on resourceId.
boolean revokeLease(final String ownerId, final String resourceId) throws LeaserException;

// Extend the lease duration held on resourceId by ownerId by an additional ttlExtendBySeconds. Return the LeaseInfo reflecting updated
// lease metadata.
LeaseInfo extendLease(final String ownerId, final String resourceId, final long ttlExtendBySeconds) throws LeaserException;

// Obtain the LeaseInfo as metadata for the lease held by ownerId on resourceId.
LeaseInfo getLeaseInfo(final String ownerId, final String resourceId) throws LeaserException;

// Get all the expired leases still held in temporary buffer by the Leaser.
Set<LeaseInfo> getExpiredLeases();

// Get all the revoked leases still held in temporary buffer by the Leaser.
Set<LeaseInfo> getRevokedLeases();

// Stop the Leaser.
void stop() throws LeaserException;

// Factory method to create a MemoryLeaser instance.
static Leaser memoryLeaser(final long maxTtlDaysAllowed, final long auditorFrequencySeconds);

// Factory method to create a PersistentLeaser instance.
static Leaser persistentLeaser(final long maxTtlDaysAllowed, final long auditorFrequencySeconds);
```

## Usage Example
```java
Leaser leaser = null;
try {
  leaser = Leaser.persistentLeaser(7L, 1L); // persistent leaser
  leaser.start(); // start the leaser
  final String ownerId = "owner";
  final long ttlSeconds = 1L;
  final Resource resource = new Resource(); // resource to acquire lease on
  final LeaseInfo leaseInfo = leaser.acquireLease(ownerId, resource.getId(), ttlSeconds); // acquire the lease
  final boolean revoked = leaser.revokeLease(ownerId, resource.getId())); // revoke the lease
} finally {
  if (leaser != null) {
    leaser.stop(); // stop the leaser
  }
}
```