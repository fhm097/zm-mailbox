package com.zimbra.cs.mailbox;

import com.zimbra.common.mailbox.MailboxLock;
import com.zimbra.common.mailbox.MailboxLockFactory;
import com.zimbra.common.util.ZimbraLog;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class DistributedMailboxLockFactory implements MailboxLockFactory {
    private final Mailbox mailbox;
    private final Config config;
    private final RedissonClient redisson;
    private RReadWriteLock writeLock;
    private final static String HOST = "redis";
    private final static String PORT = "6379";
    private RReadWriteLock readLock;
    private final ThreadLocal<Boolean> assertReadLocks = new ThreadLocal<>();

    public DistributedMailboxLockFactory(final Mailbox mailbox) {
        this.mailbox = mailbox;

        config = new Config();
        config.useSingleServer().setAddress(HOST + ":" + PORT);
        redisson = Redisson.create(config);

        try {
            writeLock = redisson.getReadWriteLock("mailbox-writer:" + this.mailbox.getAccountId());
            readLock  = redisson.getReadWriteLock("mailbox-reader:" + this.mailbox.getAccountId());
        } catch (Exception e) {
            ZimbraLog.system.fatal("Can't instantiate Redisson server", e);
            System.exit(1);
        }
    }

    @Override
    public MailboxLock readLock() {
        return new DistributedMailboxLock(readLock.readLock(), false);
    }

    @Override
    public MailboxLock writeLock() {
        return new DistributedMailboxLock(writeLock.writeLock(), true);
    }

    @Override
    @Deprecated
    public MailboxLock lock(final boolean write) {
        if (write || this.mailbox.requiresWriteLock()) {
            return writeLock();
        }
        return readLock();
    }

    @Override
    public void close() {
        redisson.shutdown();
    }
    
    public class DistributedMailboxLock implements MailboxLock {
        private final RLock lock;
        private final boolean write;

        public DistributedMailboxLock(final RLock lock, final boolean write) {
            this.lock = lock;
            this.write = write;
        }

        @Override
        public void lock() {
			assert (neverReadBeforeWrite());
			try {
				this.lock.lock();
			} finally {
				assert (!isUnlocked() || debugReleaseReadLock());
			}
        }

        @Override
        public void close() {
            this.lock.unlock();
            if(!write)
            	debugReleaseReadLock();
        }

        @Override
        public int getHoldCount() {
        		return writeLock.writeLock().getHoldCount() + readLock.readLock().getHoldCount();
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
			if (writeLock.writeLock().getHoldCount() == 0) {
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
	            if (readLock.readLock().getHoldCount() == 0) {
	                assertReadLocks.remove();
	            }
	            return true;
	        }
    }
}