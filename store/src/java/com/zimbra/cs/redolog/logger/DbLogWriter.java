package com.zimbra.cs.redolog.logger;

import com.zimbra.common.localconfig.DebugConfig;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.Constants;
import com.zimbra.common.util.Log;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.db.DbDistibutedRedolog;
import com.zimbra.cs.db.DbPool;
import com.zimbra.cs.db.DbPool.*;
import com.zimbra.cs.redolog.CommitId;
import com.zimbra.cs.redolog.RedoCommitCallback;
import com.zimbra.cs.redolog.RedoLogManager;
import com.zimbra.cs.redolog.op.CommitTxn;
import com.zimbra.cs.redolog.op.RedoableOp;
import com.zimbra.cs.util.Zimbra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class DbLogWriter implements LogWriter {

    DbConnection conn;
    private static String sServerId;
    protected RedoLogManager mRedoLogMgr;
    // Synchronizes access to mRAF, mFileSize, mLogSeq, mFsyncSeq, mLogCount, and mFsyncCount.
    private final Object mLock = new Object();
    // wait/notify between logger threads and fsync thread
    private final Object mFsyncCond = new Object();
    private FileHeader mHeader;
    private long mFirstOpTstamp;
    private long mLastOpTstamp;

    static {
        try {
            sServerId = Provisioning.getInstance().getLocalServer().getId();
        } catch (ServiceException e) {
            ZimbraLog.redolog.error("Unable to get local server ID", e);
            sServerId = "unknown";
        }
    }

    public DbLogWriter(RedoLogManager redoLogMgr) {
        mRedoLogMgr = redoLogMgr;
        mHeader = new FileHeader(sServerId);
        mCommitNotifyQueue = new CommitNotifyQueue(100);
    }

    @Override
    public void open() throws Exception {
        if (conn == null || conn.getConnection().isClosed()) {
            conn = DbPool.getConnection();
            ZimbraLog.redolog.info("fetching new DB connection");
        }
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            DbPool.quietClose(conn);
        }
    }

    @Override
    public void log(RedoableOp op, InputStream data, boolean synchronous) throws Exception {
        synchronized (mLock) {
            if (conn == null) {
                throw new Exception("Redolog connection closed");
            }

            try {
                //Record first transaction in header.
                //long tstamp = op.getTimestamp();
                //mLastOpTstamp = Math.max(tstamp, mLastOpTstamp);
                //if (mFirstOpTstamp == 0) {
                //    mFirstOpTstamp = tstamp;
                //    mHeader.setFirstOpTstamp(mFirstOpTstamp);
                //    mHeader.setLastOpTstamp(mLastOpTstamp);
                //    mHeader.serialize(mRAF);
                //}

                DbDistibutedRedolog.log_op(conn, data);
                conn.commit();
            } finally {
                data.close();
            }
        }
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public long getSize() throws Exception {
        long size;
        synchronized (mLock) {
            if (conn == null) {
                throw new Exception("Redolog connection closed");
            }
            size = DbDistibutedRedolog.getAllOpSize(conn);
        }
        return size;
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    @Override
    public long getLastLogTime() {
        return 0;
    }

    @Override
    public boolean isEmpty() throws Exception {
        return getSize() == 0;
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public String getAbsolutePath() {
        return null;
    }

    @Override
    public boolean renameTo(File dest) {
        return false;
    }

    @Override
    public boolean delete() {
        return false;
    }

    @Override
    public File rollover(LinkedHashMap activeOps) throws IOException {
        return null;
    }

    @Override
    public long getSequence() {
        return 0;
    }

    public boolean isOpen() throws SQLException {
        return (conn != null && !conn.getConnection().isClosed());
    }


/*
========================================================================================================================
====================================   TEMP CLASSES, METHODS AND FIELDS   ==============================================
===============================   ALL BELOW WAS BORROWED FROM FileLogWriter   ==========================================
========================================================================================================================
*/
//********************************************************************************************************************** FIELDS SECTION
    private CommitNotifyQueue mCommitNotifyQueue;
//********************************************************************************************************************** METHODS SECTION
//********************************************************************************************************************** INNER CLASSES SECTION

    // Commit callback handling
    private static class Notif {
        private RedoCommitCallback mCallback;
        private CommitId mCommitId;

        public Notif(RedoCommitCallback callback, CommitId cid) {
            mCallback = callback;
            mCommitId = cid;
        }

        public RedoCommitCallback getCallback() {
            return mCallback;
        }

        public CommitId getCommitId() {
            return mCommitId;
        }
    }

    private class CommitNotifyQueue {
        private Notif[] mQueue = new Notif[100];
        private int mHead;  // points to first entry
        private int mTail;  // points to just after last entry (first empty slot)
        private boolean mFull;

        public CommitNotifyQueue(int size) {
            mQueue = new Notif[size];
            mHead = mTail = 0;
            mFull = false;
        }

        public synchronized void push(Notif notif) throws IOException {
            if (notif != null) {
                if (mFull) flush();  // queue is full
                assert (!mFull);
                mQueue[mTail] = notif;
                mTail++;
                mTail %= mQueue.length;
                mFull = mTail == mHead;
            }
        }

        private synchronized Notif pop() {
            if (mHead == mTail && !mFull) return null;  // queue is empty
            Notif n = mQueue[mHead];
            mQueue[mHead] = null;  // help with GC
            mHead++;
            mHead %= mQueue.length;
            mFull = false;
            return n;
        }

        public synchronized void flush() throws IOException {
            Notif notif;
            while ((notif = pop()) != null) {
                RedoCommitCallback cb = notif.getCallback();
                assert (cb != null);
                try {
                    cb.callback(notif.getCommitId());
                } catch (OutOfMemoryError e) {
                    Zimbra.halt("out of memory", e);
                } catch (Throwable t) {
                    ZimbraLog.misc.error("Error while making commit callback", t);
                }
            }
        }
    }
}
