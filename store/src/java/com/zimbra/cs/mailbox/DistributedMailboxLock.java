package com.zimbra.cs.mailbox;

import com.zimbra.common.mailbox.MailboxLock;
import com.zimbra.common.util.ZimbraLog;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;

public class DistributedMailboxLock implements MailboxLock {
    private final RReadWriteLock readWriteLock;
    private final boolean write;
    private final RLock lock;

    //for sanity checking, we keep list of read locks. the first time caller obtains write lock they must not already own read lock
    //states - no lock, read lock only, write lock only
    private final ThreadLocal<Boolean> assertReadLocks = new ThreadLocal<>();

    public DistributedMailboxLock(final RReadWriteLock readWriteLock, final boolean write) {
        this.readWriteLock = readWriteLock;
        this.write = write;
        this.lock = this.write ? readWriteLock.writeLock() : readWriteLock.readLock();
    }

    @Override
    public void lock() {
        assert(neverReadBeforeWrite());
        try {
            this.lock.lock();
        } finally {
            assert (!isUnlocked() || debugReleaseReadLock());
        }
    }

    @Override
    public void close() {
        this.lock.unlock();
        if (!this.write) {
            assert(debugReleaseReadLock());
        }
    }

    @Override
    public int getHoldCount() {
        // eric: I feel like summing read + write lock hold count here is strange, but this is being done to
        // match the behavior of LocalMailboxLock
        return this.readWriteLock.readLock().getHoldCount() + this.readWriteLock.writeLock().getHoldCount();
    }

    @Override
    public boolean isWriteLock() {
        return this.write;
    }

    @Override
    public boolean isWriteLockedByCurrentThread() {
        if (this.write) {
            return this.lock.isHeldByCurrentThread();
        }
        return false;
    }

    @Override
    public boolean isUnlocked() {
        return !this.lock.isLocked();
    }

    private synchronized boolean neverReadBeforeWrite() {
        if (this.readWriteLock.writeLock().getHoldCount() == 0) {
            if (write) {
                Boolean readLock = assertReadLocks.get();
                if (readLock != null) {
                    ZimbraLog.mailbox.error("read lock held before write", new Exception());
                    assert (false);
                }
            } else {
                assertReadLocks.set(true);
            }
        }
        return true;
    }

    private synchronized boolean debugReleaseReadLock() {
        //remove read lock
        if (this.readWriteLock.readLock().getHoldCount() == 0) {
            assertReadLocks.remove();
        }
        return true;
    }

}