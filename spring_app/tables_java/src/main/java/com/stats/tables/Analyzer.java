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
        List<TableColumnRowCounts> tableStatistics =  this.getTableStatistics(tableSpaceName, tableOwner);
        printTableStatistics(tableStatistics);
    }

    public static void printTableStatistics(List<TableColumnRowCounts> tables) {
        System.out.println("Table name, number of columns, number of rows");
        for (TableColumnRowCounts row : tables) {
            System.out.println(row.getOwner() + " " + row.getTableName() + " " + row.getColumnCount() + " " + row.getRowCount());
        }
    }

    public List<TableColumnRowCounts> getTableStatistics(String tableSpaceName, String tableOwner) {
        String selectSql = """
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
        selectSql += " AND t.TABLESPACE_NAME = '" + tableSpaceName + "'";
        selectSql += " AND t.OWNER = '" + tableOwner + "'";
        selectSql += " ORDER BY TABLE_NAME";

        List<TableColumnRowCounts> tableColumnRowCounts = jdbcTemplate.query(selectSql, TableColumnRowCounts.getMapper());
        System.out.println("Executed query");

        return tableColumnRowCounts;
    }

    private String getTableSpaceName(String ...args) {
        return args.length == 0 ? "USERS" : args[0];
    }

    private String getTableOwner(String ...args) {
        return args.length < 2 ? "HR" : args[1];
    }
}
