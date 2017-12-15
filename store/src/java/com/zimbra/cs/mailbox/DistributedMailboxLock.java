package com.zimbra.cs.mailbox;

import com.zimbra.common.localconfig.LC;
import com.zimbra.common.mailbox.MailboxLock;
import com.zimbra.common.util.ZimbraLog;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;

import java.util.concurrent.TimeUnit;

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
        assert(neverReadBeforeWrite());
        try {
            if (!tryLockWithTimeout()) {
                throw new LockFailedException("Failed to acquire DistributedMailboxLock { \"lockId\": \"" + this.readWriteLock.getName() + "\" }");
            }
        } catch (final InterruptedException ex) {
            throw new LockFailedException("Failed to acquire DistributedMailboxLock { \"lockId\": \"" + this.readWriteLock.getName() + "\" }", ex);
        }
    }

    private boolean tryLockWithTimeout() throws InterruptedException {
        return this.lock.tryLock(LC.zimbra_mailbox_lock_timeout.intValue(), TimeUnit.SECONDS);
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

    private boolean neverReadBeforeWrite() {
        if (write && this.readWriteLock.readLock().isHeldByCurrentThread()) {
            final LockFailedException lfe = new LockFailedException("read lock held before write");
            ZimbraLog.mailbox.error(lfe.getMessage(), lfe);
            throw lfe;
        }
        return true;
    }
}