package com.stats.tables;

import org.springframework.jdbc.core.RowMapper;

public class TableColumnRowCounts {
    private String owner;
    private String tableName;
    private int columnCount;
    private int rowCount;

    public TableColumnRowCounts() {
    }

    public static RowMapper<TableColumnRowCounts> getMapper() {
        return (rs, rowNum) -> {
            TableColumnRowCounts item = new TableColumnRowCounts();
            item.setOwner(rs.getString("OWNER"));
            item.setTableName(rs.getString("TABLE_NAME"));
            item.setColumnCount(rs.getInt("NUM_COLS"));
            item.setRowCount(rs.getInt("NUM_ROWS"));
            return item;
        };
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }
}
