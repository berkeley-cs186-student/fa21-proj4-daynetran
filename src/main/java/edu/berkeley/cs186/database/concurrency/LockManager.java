package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if ((lock.transactionNum != except) && (!LockType.compatible(lockType, lock.lockType))) { // checks the lock has a different transaction than that of except and checks that the input locktype is not compatible with lock's locktype
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            Long input_tNum = lock.transactionNum;
            boolean alreadyHasLock = false;
            for (Lock resourceLock : locks) {
                // scan the resource's locks 
                if (resourceLock.transactionNum == input_tNum) {
                    // the resourceLock and input lock share transaction
                    // implies some transaction already has a lock
                    resourceLock.lockType = lock.lockType; // update the lock type on resource
                    alreadyHasLock = true;
                }
            }
            if (!alreadyHasLock) {
                // the transaction currently does not have a lock on the resource
                if (transactionLocks.keySet().contains(input_tNum)) {
                    // if the LockManager has previously worked with this transasction
                    transactionLocks.get(input_tNum).add(lock);  // add the new lock to this transaction
                    locks.add(lock);
                } else {
                    // the lockManager is working with the transaction for the first time, and so it must add it to transactionLocks Map
                    List<Lock> newLockList = new ArrayList<>(); // create a new List<Lock>
                    newLockList.add(lock); // add the new Lock to the newly created list
                    transactionLocks.put(input_tNum, newLockList); // add this Pair to the Map
                    locks.add(lock);
                }
            }
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            locks.remove(lock); // we release the lock
            transactionLocks.get(lock.transactionNum).remove(lock);
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            while (!waitingQueue.isEmpty() && checkCompatible(waitingQueue.getFirst().lock.lockType, waitingQueue.getFirst().lock.transactionNum)) {
                waitingQueue.getFirst().transaction.unblock();
                waitingQueue.removeFirst();
            }
            // // TODO(proj4_part1): implement
            // while (notEmpty)
            // while (requests.hasNext()) {
            //     LockRequest lockRequest = requests.next();
            //     boolean compatible = checkCompatible(lockRequest.lock.lockType, lockRequest.lock.transactionNum);
            //     if (compatible) {
            //         lockRequest.transaction.unblock();
            //     }
            // }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.

        boolean shouldBlock = false;

        synchronized (this) {
            ResourceEntry resource = getResourceEntry(name); // retrieve the resource of interest from its name

            // ERROR CHECK: check for DuplicateLockRequestException
            for (Lock resourceLock : resource.locks) { // skims through the resource's locks
                if (resourceLock.transactionNum == transaction.getTransNum()) { // checks for a resource lock that is held by the transaction
                    if (!releaseNames.contains(name)) { // checks that releaseNames will not be releasing this resource
                        throw new DuplicateLockRequestException("A lock on 'name' is already held by 'transaction' and isn't being released");
                    }
                }
            }
    
            // ERROR CHECK: check for NoLockHeldException
            if (releaseNames.size() > 0) {
                for (ResourceName releaseName : releaseNames) { // skim through releaseNames for each to-be-released resource
                    ResourceEntry releaseResource = getResourceEntry(releaseName); // retrieve resourceEntry from resourceName
                    if (releaseResource.getTransactionLockType(transaction.getTransNum()) == LockType.NL) {
                        // there is a lock being held on a resource-to-be-released by the transaction
                        throw new NoLockHeldException("Transaction doesn't hold a lock on one or more of the names in releaseNames.");
                    }
                }
            }
            
            // BLOCK CHECK: check for compatibility
            if (!resource.checkCompatible(lockType, transaction.getTransNum())) {
                resource.addToQueue(new LockRequest(transaction, new Lock(name, lockType, transaction.getTransNum())), true);
                shouldBlock = true;
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
        
        // Acquire
        ResourceEntry resource = getResourceEntry(name);
        resource.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));

        // Release
        for (ResourceName releasedName : releaseNames) {
            if (releasedName != name) {
                ResourceEntry releasedResourceEntry = getResourceEntry(releasedName);
                Lock toBeRemoved = null;
                for (Lock releaseLock : releasedResourceEntry.locks) {
                    if (releaseLock.transactionNum == transaction.getTransNum()) {
                        toBeRemoved = releaseLock;
                    }
                }
                if (toBeRemoved != null) {
                    releasedResourceEntry.releaseLock(toBeRemoved);
                }
            }
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        ResourceEntry resource = getResourceEntry(name);
        synchronized (this) {
            // ERROR CHECK: check for DuplicateLockRequestException
            for (Lock resourceLock : resource.locks) { // skims through the resource's locks
                if (resourceLock.transactionNum == transaction.getTransNum()) { // checks for a resource lock whose transaction is the same as the input transaction
                    throw new DuplicateLockRequestException("A lock on 'name' is already held by 'transaction'");
                }
            }

            // BLOCK CHECK: check if the resource cannot accept this new lock or if there are already other locks waiting
            if ( (!resource.checkCompatible(lockType, transaction.getTransNum())) || (!resource.waitingQueue.isEmpty()) ) {
                transaction.prepareBlock();
                resource.addToQueue(new LockRequest(transaction, new Lock(name, lockType, transaction.getTransNum())), false);
                shouldBlock = true;
                
            }
        }
        if (shouldBlock) {
            transaction.block();
        }

        // Once unblocked, grant/update the lock
        Lock newLock = new Lock(name, lockType, transaction.getTransNum());
        resource.grantOrUpdateLock(newLock);
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            ResourceEntry resource = getResourceEntry(name);

            if (resource.getTransactionLockType(transaction.getTransNum()) == LockType.NL) {
                throw new NoLockHeldException("No lock on name is held by transaction");
            }

            resource.releaseLock(new Lock(name, resource.getTransactionLockType(transaction.getTransNum()), transaction.getTransNum()));
            resource.processQueue();
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry resource = getResourceEntry(name);
            List<Lock> resourceLocks = getLocks(name);
            // ERROR CHECK: throws DuplicateLockRequestException if `transaction` already has a 'newLockType` lock       
            // ERROR CHECK: throws NoLockHeldException if `transaction` has no lock on `name`  
            Lock currentTransactionLock = null;
            for (Lock resourceLock : resourceLocks) {
                if (resourceLock.transactionNum == transaction.getTransNum()) {
                    currentTransactionLock = resourceLock;
                }
            }
            if (currentTransactionLock == null || currentTransactionLock.lockType == LockType.NL) {
                throw new NoLockHeldException("transaction has no lock");
            }
            if (currentTransactionLock.lockType == newLockType) {
                throw new DuplicateLockRequestException("Transaction already has a newLockType lock");
            }
            if (!LockType.substitutable(newLockType, currentTransactionLock.lockType)) {
                throw new InvalidLockException("requested lock type is not a promotion");
            }
            if (!resource.checkCompatible(newLockType, transaction.getTransNum())) {
                // the lock-to-be-promoted is not compatible with other transactions' locks
                // we will add the lockrequest to front of queue and prepare to block
                resource.addToQueue(new LockRequest(transaction, new Lock(name, newLockType, transaction.getTransNum())), true); // 
                shouldBlock = true;
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }       
        // Once unblocked, promote the lock
        ResourceEntry resource = getResourceEntry(name);
        resource.grantOrUpdateLock(new Lock(name, newLockType, transaction.getTransNum()));
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        for (Lock resourceLock : resourceEntry.locks) {
            if (resourceLock.transactionNum == transaction.getTransNum()) {
                return resourceLock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
