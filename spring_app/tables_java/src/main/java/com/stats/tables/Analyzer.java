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

        List<TableColumnRowCounts> tableColumnRowCounts = jdbcTemplate.query(selectSql, TableColumnRowCounts.getMapper());
        System.out.println("Executed query");

        return tableColumnRowCounts;
    }
}
