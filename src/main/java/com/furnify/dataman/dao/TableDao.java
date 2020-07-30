package com.furnify.dataman.dao;

import lombok.Cleanup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableDao {

    private final Connection connection;

    public TableDao(Connection connection) {
        this.connection = connection;
    }

    public boolean tableExists(String schemaName, String tableName) throws SQLException {
        try (PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
            pstmt.setString(1, schemaName);
            pstmt.setString(2, tableName);

            ResultSet resultSet = pstmt.executeQuery();
            return resultSet.next();
        }
    }

    public boolean recordExists(String query) throws SQLException {
        try (PreparedStatement selectStatement = connection.prepareStatement(query)) {
            ResultSet resultSet = selectStatement.executeQuery();
            return resultSet.next();
        }
    }

    public int executeInsert(String query) throws SQLException {
        return executeUpdate(query);
    }

    public int executeUpdate(String query) throws SQLException {
        try (PreparedStatement updateStatement = connection.prepareStatement(query)) {
            return updateStatement.executeUpdate();
        }
    }

}
