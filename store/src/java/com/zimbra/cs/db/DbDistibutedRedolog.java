package com.zimbra.cs.db;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.db.DbPool.DbConnection;


/**
 * distibuted_redolog table.
 *
 * @since 2018. 01. 04.
 */
public final class DbDistibutedRedolog {

    public static void log_op(DbConnection conn, InputStream op) throws ServiceException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("INSERT INTO distributed_redolog (op) VALUES (?)");
            stmt.setBinaryStream(1, op);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw ServiceException.FAILURE("Log Redo Op ", e);
        } finally {
            DbPool.closeStatement(stmt);
        }
    }

//    public static Map<Long, InputStream> getAllOp(DbConnection conn) throws ServiceException {
//        PreparedStatement stmt = null;
//        ResultSet rs = null;
//        Map<Long, InputStream> allOp = new HashMap<>();
//        try {
//            stmt = conn.prepareStatement("SELECT * FROM distributed_redolog");
//            rs = stmt.executeQuery();
//            while (rs.next()) {
//                allOp.put(rs.getLong("opOrder"), rs.getBinaryStream("op"));
//            }
//        } catch (SQLException e) {
//            throw ServiceException.FAILURE("Getting All Redo Op ", e);
//        } finally {
//            DbPool.closeResults(rs);
//            DbPool.closeStatement(stmt);
//        }
//        return allOp;
//    }

    public static long getAllOpSize(DbConnection conn) throws ServiceException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        long size = 0;
        try {
            stmt = conn.prepareStatement("Select SUM(OCTET_LENGTH(op)) from distributed_redolog;");
            rs = stmt.executeQuery();
            if (rs.next()){
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
}
