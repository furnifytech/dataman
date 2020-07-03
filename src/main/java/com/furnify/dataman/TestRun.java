package com.furnify.dataman;

import com.furnify.dataman.processor.DataUpdater;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestRun {

    public static void main(String args[]) {
        Connection connection = getConnection();
        DataUpdater.importFromXML(connection, Paths.get("D:\\Repo\\centrally_server\\src\\main\\resources\\dataman\\menu_master.xml"));
    }

    private static Connection getConnection() {
        try {
            return DriverManager.getConnection("jdbc:mysql://localhost/centrally?serverTimezone=Asia/Kolkata",
                    "centrally_dbuser", "CentrallyDB@2020");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

}
