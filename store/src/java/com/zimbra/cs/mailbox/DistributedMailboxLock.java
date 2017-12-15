package com.zimbra.cs.mailbox;

import com.zimbra.common.mailbox.MailboxLock;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;

public class DistributedMailboxLock implements MailboxLock {
    private final RReadWriteLock readWriteLock;
    private final boolean write;
    private final RLock lock;

    public DistributedMailboxLock(final RReadWriteLock readWriteLock, final boolean write) {
        this.readWriteLock = readWriteLock;
        this.write = write;
        this.lock = this.write ? readWriteLock.writeLock() : readWriteLock.readLock();
    }

    @Override
    public void lock() {
        this.lock.lock();
    }

    @Override
    public void close() {
        this.lock.unlock();
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
}