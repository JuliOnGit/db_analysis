package com.stats.tables;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Component
public class Analyzer implements CommandLineRunner {

    @Autowired
    private DataSource dataSource;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Hello from run(...)");
        List<TableColumnRowCounts> tableStatistics =  this.getTableStatistics();
        printTableStatistics(tableStatistics);

    }

    public static void printTableStatistics(List<TableColumnRowCounts> tables) {
        System.out.println("Table name, number of columns, number of rows");
        for (TableColumnRowCounts row : tables) {
            System.out.println(row.getTableName() + " " + row.getColumnCount() + " " + row.getRowCount());
        }
    }

    public List<TableColumnRowCounts> getTableStatistics() {
        // driverType "oci" employs IPC (inter process communication) and is recommended,
        // when JDBC-Client and oracle-Server runs on the same machine.
//        String driverType = "thin";
//        String host = "localhost";
//        String port = "1521";
//        String sid = "freepdb1";
//        String username = "hr";
//        String pw = "oracle";
//
//        String url = String.format("jdbc:oracle:%s:%s/%s@%s:%s/%s", driverType, username, pw, host, port, sid);

//        try (Connection connection = DriverManager.getConnection(url)) {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();

             String selectSql = """
                     SELECT t.TABLE_NAME, c.NUM_COLS, r.NUM_ROWS
                     FROM
                       ALL_TABLES t,
                       ALL_TAB_STATISTICS r,
                       (SELECT s.TABLE_NAME, COUNT(*) NUM_COLS
                       FROM ALL_TAB_COL_STATISTICS s
                       GROUP BY s.TABLE_NAME) c
                     WHERE t.TABLE_NAME = r.TABLE_NAME
                       AND t.TABLE_NAME = c.TABLE_NAME
                       AND t.TABLESPACE_NAME = 'USERS'
                     """;
//            String selectSql = "SELECT t.TABLE_NAME, c.NUM_COLS, r.NUM_ROWS "
//                    + "FROM"
//                    + "  ALL_TABLES t, "
//                    + "  ALL_TAB_STATISTICS r, "
//                    + "  (SELECT s.TABLE_NAME, COUNT(*) NUM_COLS"
//                    + "  FROM ALL_TAB_COL_STATISTICS s"
//                    + "  GROUP BY s.TABLE_NAME) c "
//                    + "WHERE t.TABLE_NAME = r.TABLE_NAME"
//                    + "  AND t.TABLE_NAME = c.TABLE_NAME"
//                    + "  AND t.TABLESPACE_NAME = 'USERS'";

            ResultSet queryResult = statement.executeQuery(selectSql);
            System.out.println("Executed query");
            List<TableColumnRowCounts> tableColumnRowCounts = new ArrayList<>();

            while (queryResult.next()) {
                TableColumnRowCounts entry = new TableColumnRowCounts();
                entry.setTableName(queryResult.getString("TABLE_NAME"));
                entry.setColumnCount(queryResult.getInt("NUM_COLS"));
                entry.setRowCount(queryResult.getInt("NUM_ROWS"));
                tableColumnRowCounts.add(entry);
            }

            return tableColumnRowCounts;

        } catch (SQLException e) {
            System.out.println("Got an exception");
            throw new RuntimeException(e);
        } finally {
            System.out.println("In finally block");
        }
    }
}
