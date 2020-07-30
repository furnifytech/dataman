package com.furnify.dataman;

import com.furnify.dataman.processor.DataUpdater;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestRun {

    private static final Logger logger = Logger.getLogger(TestRun.class.getName());

    public static void main(String[] args) {
        Connection connection = getConnection();
        DataUpdater.importFromXML(connection, Paths.get("D:\\Repo\\centrally_server\\src\\main\\resources\\dataman\\scheduler.xml"), "dev");
    }

    private static Connection getConnection() {
        try {
            return DriverManager.getConnection("jdbc:mysql://localhost/centrally_dev?serverTimezone=Asia/Kolkata",
                    "centrally_dbuser", "CentrallyDB@2020");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Exception in Getting connection", e);
        }

        return null;
    }

}
