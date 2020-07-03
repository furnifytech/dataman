package com.furnify.dataman.dao;

import lombok.Cleanup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableDao {

    public static boolean tableExists(Connection con, String schemaName, String tableName) throws SQLException {
        @Cleanup PreparedStatement pstmt = con.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?");
        pstmt.setString(1, schemaName);
        pstmt.setString(2, tableName);
        @Cleanup ResultSet resultSet = pstmt.executeQuery();
        if (resultSet.next()) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean recordExists(Connection con, String query) throws SQLException {
        @Cleanup PreparedStatement pstmt = con.prepareStatement(query);
        @Cleanup ResultSet resultSet = pstmt.executeQuery();
        if (resultSet.next()) {
            return true;
        } else {
            return false;
        }
    }

    public static int executeInsert(Connection con, String query) throws SQLException {
        return executeUpdate(con, query);
    }

    public static int executeUpdate(Connection con, String query) throws SQLException {
        @Cleanup PreparedStatement pstmt = con.prepareStatement(query);
        return pstmt.executeUpdate();
    }

}
