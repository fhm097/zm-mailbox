package com.zimbra.cs.db;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.db.DbPool.DbConnection;


/**
 * distibuted_redolog table.
 *
 * @since 2018. 01. 04.
 */
public final class DbDistibutedRedolog {

    public enum OpType {
        HEADER("HD"), OPERATION("OP");

        private String dbValue;

        private OpType(String opType) {
            this.dbValue = opType;
        }

        public String getDbValue() {
            return dbValue;
        }
    }

    public static void logOp(DbConnection conn, OpType opType, InputStream op) throws ServiceException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("INSERT INTO distributed_redolog (opType, op) VALUES (?,?)");
            stmt.setString(1, opType.getDbValue());
            stmt.setBinaryStream(2, op);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw ServiceException.FAILURE("Log Redo Op ", e);
        } finally {
            DbPool.closeStatement(stmt);
        }
    }

    // TODO this method is not tested jet and maybe will not be needed
    public static Map<Long, InputStream> getAllOp(DbConnection conn) throws ServiceException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Map<Long, InputStream> allOp = new HashMap<>();
        try {
            stmt = conn.prepareStatement("SELECT * FROM distributed_redolog");
            rs = stmt.executeQuery();
            while (rs.next()) {
                allOp.put(rs.getLong("opOrder"), rs.getBinaryStream("op"));
            }
        } catch (SQLException e) {
            throw ServiceException.FAILURE("Getting All Redo Op ", e);
        } finally {
            DbPool.closeResults(rs);
            DbPool.closeStatement(stmt);
        }
        return allOp;
    }

    public static long getAllOpSize(DbConnection conn) throws ServiceException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        long size = 0;
        try {
            stmt = conn.prepareStatement("Select SUM(OCTET_LENGTH(op)) from distributed_redolog;");
            rs = stmt.executeQuery();
            if (rs.next()) {
                size = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw ServiceException.FAILURE("Getting All Redo Op ", e);
        } finally {
            DbPool.closeResults(rs);
            DbPool.closeStatement(stmt);
        }
        return size;
    }

    public static InputStream getHeaderOp(DbConnection conn) throws ServiceException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        InputStream header = null;
        try {
            stmt = conn.prepareStatement("SELECT op FROM distributed_redolog WHERE  opType = ?;");
            stmt.setString(1, OpType.HEADER.getDbValue());
            rs = stmt.executeQuery();
            if (rs.next()) {
                header = rs.getBinaryStream(1);
            }
        } catch (SQLException e) {
            throw ServiceException.FAILURE("Getting Header Redo Op ", e);
        } finally {
            DbPool.closeResults(rs);
            DbPool.closeStatement(stmt);
        }
        return header;
    }
}
