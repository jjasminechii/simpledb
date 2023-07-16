package simpledb;

import java.io.*;
import java.util.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, TransactionId> exclusive;  // for writes
    private Map<PageId, Set<TransactionId>> shared;  // for reads
    private Map<TransactionId, Set<TransactionId>> dependencyGraph;  // for deadlock detection

    public LockManager() {
        exclusive = new ConcurrentHashMap<>();
        shared = new ConcurrentHashMap<>();
        dependencyGraph = new ConcurrentHashMap<>();
    }

    // Returns true if the transaction currently has an exclusive lock on the page.
    // Basically checks if it has write permission
    public synchronized boolean hasExclusiveLock(TransactionId tid, PageId page) {
        return exclusive.containsKey(page) && exclusive.get(page).equals(tid);
    }

    // Returns true if the transaction currently has a shared lock on the page.
    // Basically checks if it has read permission
    public synchronized boolean hasSharedLock(TransactionId tid, PageId page) {
        if (shared.containsKey(page)) {
            Set<TransactionId> transactions = shared.get(page);
            return transactions.contains(tid);
        }
        return false;
    }

    // Adds a lock to the shared locks map
    public synchronized void addSharedLock(TransactionId tid, PageId page) {
        if (shared.containsKey(page)) {
            shared.get(page).add(tid);
        } else {
            Set<TransactionId> transactionIds = new HashSet<>();
            transactionIds.add(tid);
            shared.put(page, transactionIds);
        }
    }

    // Gets an exclusive lock if the transaction is doing a write, or a shared lock if doing a read.
    // Returns true if successful
    public synchronized boolean acquireLock(TransactionId tid, PageId page, Permissions permissionType) throws TransactionAbortedException {
        if (permissionType == Permissions.READ_ONLY) {
            while (!acquireSharedLock(tid, page)) {
                // wait until transaction gets a lock
            }
            return true;
        } else if (permissionType == Permissions.READ_WRITE) {
            while (!acquireExclusiveLock(tid, page)) {
                // wait until transaction gets a lock
            }
            return true;
        } else {
            throw new IllegalArgumentException("Permission must be READ_ONLY or READ_WRITE");
        }
    }

    // Returns true if the transaction was able to get a lock on the page
    // Returns false if the transaction needs to wait
    public synchronized boolean acquireSharedLock(TransactionId tid, PageId page) throws TransactionAbortedException {
        // trying to get shared lock on page with exclusive lock:
        if (exclusive.containsKey(page)) {
            // if the exclusive lock is held by this transaction, can also safely acquire read lock. no dependencies
            if (exclusive.get(page).equals(tid)) {
                // acquire read lock
                addSharedLock(tid, page);
                return true;

                // if the exclusive lock is held by a different transaction t2, need to wait
            } else {
                // get the transaction holding the exclusive lock on page
                TransactionId holdsExclusiveLock = exclusive.get(page);
                // check if adding tid -> t2 to graph results in deadlock
                boolean deadlock = detectDeadlock(tid, holdsExclusiveLock);
                if (deadlock) {
                    throw new TransactionAbortedException();
                } else {
                    // if no deadlock will happen, add dependency and wait
                    addDependency(tid, holdsExclusiveLock);
                    return false;
                }
            }
        }

        // if no exclusive lock on this page, can acquire a read lock regardless of whether
        // the page has no locks or multiple shared locks already. no dependencies
        addSharedLock(tid, page);
        return true;
    }

    public synchronized boolean acquireExclusiveLock(TransactionId tid, PageId page) throws TransactionAbortedException {
        // if there's an exclusive lock on the page already and it's held by a different transaction
        if (exclusive.containsKey(page) && !exclusive.get(page).equals(tid)) {
            TransactionId holdsExclusiveLock = exclusive.get(page);
            boolean deadlock = detectDeadlock(tid, holdsExclusiveLock);
            if (deadlock) {
                throw new TransactionAbortedException();
            }
            // no deadlock will happen, so add to dependency graph and wait
            addDependency(tid, holdsExclusiveLock);
            return false;
        }

        // there's no exclusive lock on page
        // if no shared locks at all (aka no locks)
        if (!shared.containsKey(page)) {
            exclusive.put(page, tid);
            return true;
        } else {
            // if page exists in shared map but its mapped to an empty set, that means there's no shared locks
            if (shared.get(page).isEmpty()) {
                exclusive.put(page, tid);
                return true;
                // only one shared lock on it held by tid -> upgrade to exclusive lock
            } else if (shared.get(page).size() == 1 && shared.get(page).contains(tid)) {
                exclusive.put(page, tid);
                shared.get(page).remove(tid);
                return true;
                // else set must not be empty aka already multiple shared locks on it held by others (ex T2, T3, T4)
            } else {
                Set<TransactionId> holdsSharedLock = shared.get(page);
                // Check if adding T1 → T2, T1 → T3, or T1 → T4 will result in deadlock
                for (TransactionId txnHoldingSharedLock : holdsSharedLock) {
                    if (detectDeadlock(tid, txnHoldingSharedLock)) {
                        throw new TransactionAbortedException();
                    }
                }
                // Add T1 → T2, T1 → T3, or T1 → T4 to graph and wait for all of them to be released
                for (TransactionId txnHoldingSharedLock : holdsSharedLock) {
                    addDependency(tid, txnHoldingSharedLock);
                }
                return false;
            }
        }
    }

    public synchronized boolean releaseSharedLock(TransactionId tid, PageId page) {
        // release shared lock on tid by removing the tid from the set of transactionIDs
        if (shared.containsKey(page)) {
            return shared.get(page).remove(tid);
        }
        return false;
    }

    public synchronized boolean releaseExclusiveLock(TransactionId tid, PageId page) {
        // release exclusive lock by removing page from the map
        if (exclusive.containsKey(page)) {
            if (exclusive.get(page).equals(tid)) {
                exclusive.remove(page);
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    // Adds a connection to the dependency graph
    // Can only call this if detectDeadlock(src, dest) was true
    public synchronized void addDependency(TransactionId src, TransactionId dest) {
//        assert detectDeadlock(src, dest);

        // add src --> dest mapping to graph
        if (!dependencyGraph.containsKey(src)) {
            Set<TransactionId> newMapping = new HashSet<>();
            dependencyGraph.put(src, newMapping);
        }
        Set<TransactionId> tids = dependencyGraph.get(src);
        tids.add(dest);
        dependencyGraph.put(src, tids);
    }

    // checks if adding t1 -> t2 to dependency graph results in a cycle
    public synchronized boolean detectDeadlock(TransactionId t1, TransactionId t2) {
        // adding t1 -> t2 would result in cycle if t2 -> t1 already exists
        if (dependencyGraph.containsKey(t2)) {
            Set<TransactionId> t2Mappings = dependencyGraph.get(t2);
            return t2Mappings.contains(t1);
        }
        return false;
    }

    // removes tid completely
    public synchronized void removeDependency(TransactionId tid) {
        // remove all values
        for (TransactionId t : dependencyGraph.keySet()) {
            dependencyGraph.get(t).remove(tid);
        }
        // remove key
        dependencyGraph.remove(tid);
    }

    // cleans up the locks by removing all locks involving tid
    public synchronized void finishTransaction(TransactionId tid) {
        // clean up shared locks
        for (PageId page: shared.keySet()) {
            shared.get(page).remove(tid);
            shared.put(page, shared.get(page));
        }

        // clean up exclusive locks
        Set<PageId> locksToRemove = new HashSet<PageId>();
        for (PageId page : exclusive.keySet()) {
            if (tid.equals(exclusive.get(page))) {
                locksToRemove.add(page);
            }
        }
        for (PageId page: locksToRemove) {
            exclusive.remove(page);
        }
    }
}