package com.zimbra.cs.redolog.logger;

import com.zimbra.common.localconfig.DebugConfig;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.Constants;
import com.zimbra.common.util.Log;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.db.DbDistibutedRedolog;
import com.zimbra.cs.db.DbDistibutedRedolog.OpType;
import com.zimbra.cs.db.DbPool;
import com.zimbra.cs.db.DbPool.*;
import com.zimbra.cs.redolog.*;
import com.zimbra.cs.redolog.op.CommitTxn;
import com.zimbra.cs.redolog.op.RedoableOp;
import com.zimbra.cs.util.Zimbra;

import java.io.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DbLogWriter implements LogWriter {

    DbConnection conn;
    private LogHeader mHeader;
    protected RedoLogManager mRedoLogMgr;
    // Synchronizes access to mRAF, mFileSize, mLogSeq, mFsyncSeq, mLogCount, and mFsyncCount.
    private final Object mLock = new Object();

    private static String sServerId;
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
        mHeader = new LogHeader(sServerId);
    }

    @Override
    public void open() throws Exception {
        if (conn != null && !conn.getConnection().isClosed()) {
            return; // already open
        }

        conn = DbPool.getConnection();
        ZimbraLog.redolog.info("fetching new DB connection");
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

                DbDistibutedRedolog.logOp(conn, OpType.OPERATION, data);
                conn.commit();
            } finally {
                try {
                    data.close();
                } catch (IOException e) {
                    ZimbraLog.redolog.error("Failed to close Op's data Stream", e);
                }
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
========================   ALL BELOW WAS BORROWED FROM FileLogWriter and FileHeader   ==================================
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

    public static class LogHeader {

        public static final int HEADER_LEN = 512;
        private static final int SERVER_ID_FIELD_LEN = 127;
        private final byte[] MAGIC = "ZM_REDO".getBytes();

        private byte mOpen;                 // logfile is open or closed
        private long mFileSize;             // filesize
        private long mSeq;                  // log file sequence number
        private String mServerId;           // host on which the file was created. zimbraId attribute from LDAP
        private long mFirstOpTstamp;        // time of first op in log file
        private long mLastOpTstamp;         // time of last op in log file
        private long mCreateTime;           // create time of log file

        private Version mVersion;            // redo log version

        public String DATE_FORMAT = "EEE, yyyy/MM/dd HH:mm:ss.SSS z";

        LogHeader() {
            this("unknown");
        }

        LogHeader(String serverId) {
            mServerId = serverId;
            mVersion = Version.latest();
        }

        public void init(DbConnection conn) throws Exception {
            InputStream header = DbDistibutedRedolog.getHeaderOp(conn);

            if (header != null) {
                read(header);
            } else {
                mOpen = 1;
                mFileSize = 0;
                mSeq = 0;
                mFirstOpTstamp = 0;
                mLastOpTstamp = 0;
                mCreateTime = System.currentTimeMillis();

                write(conn);
            }
        }

        void write(DbConnection conn) throws Exception {
            // Update header redolog version to latest code version.
            if (!mVersion.isLatest()) {
                mVersion = Version.latest();
            }

            InputStream data = new ByteArrayInputStream(serialize());

            try {
                DbDistibutedRedolog.logOp(conn, OpType.HEADER, data);
                conn.commit();
            } finally {
                try {
                    data.close();
                } catch (IOException e) {
                    ZimbraLog.redolog.error("Failed to close Header's data Stream", e);
                }
            }
        }

        void read(DbConnection conn) throws Exception {
            InputStream headerData = DbDistibutedRedolog.getHeaderOp(conn);
            byte[] header = readBytesFromInputStream(headerData);
            deserialize(header);
        }

        private void read(InputStream headerData) throws Exception {
            byte[] header = readBytesFromInputStream(headerData);
            deserialize(header);
        }

        private byte[] readBytesFromInputStream(InputStream headerData) throws Exception {
            byte[] header = new byte[HEADER_LEN];
            try {
                int bytesRead = headerData.read(header, 0, HEADER_LEN);

                if (bytesRead < HEADER_LEN) {
                    throw new Exception("Redolog is smaller than header length of " + HEADER_LEN + " bytes");
                }

                return header;
            } finally {
                try {
                    headerData.close();
                } catch (IOException e) {
                    ZimbraLog.redolog.error("Failed to close Header's data Stream", e);
                }
            }
        }

        void setOpen(boolean b) {
            if (b)
                mOpen = (byte) 1;
            else
                mOpen = (byte) 0;
        }

        void setFileSize(long s) {
            mFileSize = s;
        }

        void setSequence(long seq) {
            mSeq = seq;
        }

        void setFirstOpTstamp(long t) {
            mFirstOpTstamp = t;
        }

        void setLastOpTstamp(long t) {
            mLastOpTstamp = t;
        }

        void setCreateTime(long t) {
            mCreateTime = t;
        }

        public boolean getOpen() {
            return mOpen != 0;
        }

        public long getFileSize() {
            return mFileSize;
        }

        public long getSequence() {
            return mSeq;
        }

        public String getServerId() {
            return mServerId;
        }

        public long getFirstOpTstamp() {
            return mFirstOpTstamp;
        }

        public long getLastOpTstamp() {
            return mLastOpTstamp;
        }

        public long getCreateTime() {
            return mCreateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof com.zimbra.cs.redolog.logger.DbLogWriter.LogHeader) || o == null) {
                return false;
            }
            com.zimbra.cs.redolog.logger.DbLogWriter.LogHeader oHdr = (com.zimbra.cs.redolog.logger.DbLogWriter.LogHeader) o;
            return mOpen == oHdr.mOpen &&
                    mFileSize == oHdr.mFileSize &&
                    mSeq == oHdr.mSeq &&
                    mServerId.equals(oHdr.mServerId) &&
                    mFirstOpTstamp == oHdr.mFirstOpTstamp &&
                    mLastOpTstamp == oHdr.mLastOpTstamp &&
                    mCreateTime == oHdr.mCreateTime &&
                    mVersion.equals(oHdr.mVersion);
        }

        /**
         * Get byte buffer of a String that fits within given maximum length.
         * String is trimmed at the end one character at a time until the
         * byte representation in given charset fits maxlen.
         *
         * @param str
         * @param charset
         * @param maxlen
         * @return byte array of str in charset encoding;
         * zero-length array if any trouble
         */
        private byte[] getStringBytes(String str, String charset, int maxlen) {
            String substr = str;
            int len = substr.length();
            while (len > 0) {
                byte[] buf = null;
                try {
                    buf = substr.getBytes(charset);
                } catch (UnsupportedEncodingException e) {
                    // Treat as if we had 0-length string.
                    break;
                }
                if (buf.length <= maxlen)
                    return buf;
                substr = substr.substring(0, --len);
            }
            byte[] buf = new byte[0];
            return buf;
        }


        private byte[] serialize() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(HEADER_LEN);
            RedoLogOutput out = new RedoLogOutput(baos);
            out.write(MAGIC);
            out.writeByte(mOpen);
            out.writeLong(mFileSize);
            out.writeLong(mSeq);

            // ServerId field:
            //
            //   length   (byte)   length of serverId in bytes
            //   serverId (byte[]) bytes in UTF-8;
            //                     up to SERVER_ID_FIELD_LEN bytes
            //   padding  (byte[]) optional; 0 bytes to make
            //                     length(serverId + padding) = SERVER_ID_FIELD_LEN
            byte[] serverIdBuf =
                    getStringBytes(mServerId, "UTF-8", SERVER_ID_FIELD_LEN);
            out.writeByte((byte) serverIdBuf.length);
            out.write(serverIdBuf);
            if (serverIdBuf.length < SERVER_ID_FIELD_LEN) {
                byte[] padding = new byte[SERVER_ID_FIELD_LEN - serverIdBuf.length];
                Arrays.fill(padding, (byte) 0); // might not be necessary
                out.write(padding);
            }

            out.writeLong(mFirstOpTstamp);
            out.writeLong(mLastOpTstamp);
            mVersion.serialize(out);
            out.writeLong(mCreateTime);

            int currentLen = baos.size();
            if (currentLen < HEADER_LEN) {
                int paddingLen = HEADER_LEN - currentLen;
                byte[] b = new byte[paddingLen];
                Arrays.fill(b, (byte) 0);
                out.write(b);
            }

            byte[] headerBuf = baos.toByteArray();
            baos.close();
            if (headerBuf.length != HEADER_LEN)
                throw new IOException("Wrong redolog header length of " +
                        headerBuf.length + "; should be " +
                        HEADER_LEN);
            return headerBuf;
        }

        private void deserialize(byte[] headerBuf) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(headerBuf);
            RedoLogInput in = new RedoLogInput(bais);

            try {
                byte[] magic = new byte[MAGIC.length];
                in.readFully(magic, 0, MAGIC.length);
                if (!Arrays.equals(magic, MAGIC)) {
                    throw new IOException("Missing magic bytes in redolog header");
                }

                mOpen = in.readByte();
                mFileSize = in.readLong();
                mSeq = in.readLong();

                int serverIdLen = (int) in.readByte();
                if (serverIdLen > SERVER_ID_FIELD_LEN) {
                    throw new IOException("ServerId too long (" + serverIdLen + " bytes) in redolog header");
                }
                byte[] serverIdBuf = new byte[SERVER_ID_FIELD_LEN];
                in.readFully(serverIdBuf, 0, SERVER_ID_FIELD_LEN);
                mServerId = new String(serverIdBuf, 0, serverIdLen, "UTF-8");

                mFirstOpTstamp = in.readLong();
                mLastOpTstamp = in.readLong();

                mVersion.deserialize(in);
                if (mVersion.tooHigh()) {
                    throw new IOException("Redo log version " + mVersion + " is higher than the highest known version " +
                            Version.latest());
                }
                // Versioning of file header was added late in the game.
                // Any redolog files created previously will have version 0.0.
                // Assume version 1.0 for those files.
                if (!mVersion.atLeast(1, 0))
                    mVersion = new Version(1, 0);

                mCreateTime = in.readLong();
            } finally {
                bais.close();
            }
        }

        public String toString() {
            SimpleDateFormat fmt = new SimpleDateFormat(DATE_FORMAT);
            StringBuilder sb = new StringBuilder(100);
            sb.append("sequence: ").append(mSeq).append("\n");
            sb.append("open:     ").append(mOpen).append("\n");
            sb.append("filesize: ").append(mFileSize).append("\n");
            sb.append("serverId: ").append(mServerId).append("\n");
            sb.append("created:  ");
            sb.append(fmt.format(new Date(mCreateTime))).append(" (").append(mCreateTime).append(")");
            sb.append("\n");
            sb.append("first op: ");
            sb.append(fmt.format(new Date(mFirstOpTstamp))).append(" (").append(mFirstOpTstamp).append(")");
            sb.append("\n");
            sb.append("last op:  ");
            sb.append(fmt.format(new Date(mLastOpTstamp))).append(" (").append(mLastOpTstamp).append(")");
            if (mOpen != 0)
                sb.append(" (not up to date)");

            sb.append("\n");
            sb.append("version:  ").append(mVersion).append("\n");
            return sb.toString();
        }
    }
}