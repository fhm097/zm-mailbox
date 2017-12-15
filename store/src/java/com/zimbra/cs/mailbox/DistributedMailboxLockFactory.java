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
    private RReadWriteLock readWriteLock;

    private final static String HOST = "redis";
    private final static String PORT = "6379";

    public DistributedMailboxLockFactory(final Mailbox mailbox) {
        this.mailbox = mailbox;

        this.config = new Config();
        this.config.useSingleServer().setAddress("redis://" + HOST + ":" + PORT);
        this.redisson = Redisson.create(this.config);

        try {
            this.readWriteLock = this.redisson.getReadWriteLock("mailbox:" + this.mailbox.getAccountId());
        } catch (Exception e) {
            ZimbraLog.system.fatal("Can't instantiate Redisson server", e);
            System.exit(1);
        }
    }

    @Override
    public MailboxLock readLock() {
        return new DistributedMailboxLock(this.readWriteLock, false);
    }

    @Override
    public MailboxLock writeLock() {
        return new DistributedMailboxLock(this.readWriteLock, true);
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
        this.redisson.shutdown();
    }
}