package com.stats.tables;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class Analyzer implements CommandLineRunner {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... args) {
        String tableSpaceName = getTableSpaceName(args);
        String tableOwner = getTableOwner(args);

        dropTable();
        createTable(tableSpaceName, tableOwner);
        List<TableColumnRowCounts> tableStatistics =  this.getTableStatistics();

        printTableStatistics(tableStatistics);
    }

    public void dropTable() {
        String sql = """
                BEGIN
                   EXECUTE IMMEDIATE 'DROP TABLE TAB_COL_ROW_COUNTS';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;
                """;

        jdbcTemplate.execute(sql);
    }

    public void createTable(String tableSpaceName, String tableOwner) {
        String sql = """
                     CREATE TABLE TAB_COL_ROW_COUNTS AS
                     SELECT t.OWNER, t.TABLE_NAME, c.NUM_COLS, r.NUM_ROWS
                     FROM
                       ALL_TABLES t,
                       ALL_TAB_STATISTICS r,
                       (SELECT s.OWNER, s.TABLE_NAME, COUNT(*) NUM_COLS
                       FROM ALL_TAB_COLUMNS s
                       GROUP BY s.OWNER, s.TABLE_NAME) c
                     WHERE t.TABLE_NAME = r.TABLE_NAME
                       AND t.TABLE_NAME = c.TABLE_NAME
                       AND t.OWNER = r.OWNER
                       AND t.OWNER = c.OWNER
                     """;
        sql += " AND t.TABLESPACE_NAME = '" + tableSpaceName + "'";
        sql += " AND t.OWNER = '" + tableOwner + "'";
        sql += " ORDER BY TABLE_NAME";

        jdbcTemplate.execute(sql);
    }

    public List<TableColumnRowCounts> getTableStatistics() {
        String sql = "SELECT OWNER, TABLE_NAME, NUM_COLS, NUM_ROWS FROM TAB_COL_ROW_COUNTS";
        List<TableColumnRowCounts> tableColumnRowCounts = jdbcTemplate.query(sql, TableColumnRowCounts.getMapper());
        System.out.println("Executed query");

        return tableColumnRowCounts;
    }

    public static void printTableStatistics(List<TableColumnRowCounts> tables) {
        System.out.println("Table name, number of columns, number of rows");
        for (TableColumnRowCounts row : tables) {
            System.out.println(row.getOwner() + " " + row.getTableName() + " " + row.getColumnCount() + " " + row.getRowCount());
        }
    }

    private String getTableSpaceName(String ...args) {
        return args.length == 0 ? "USERS" : args[0];
    }

    private String getTableOwner(String ...args) {
        return args.length < 2 ? "HR" : args[1];
    }
}
