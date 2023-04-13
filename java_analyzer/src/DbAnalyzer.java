import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DbAnalyzer {
    public List<TableColumnRowCounts> getTableStatistics() {
        // driverType "oci" employs IPC (inter process communication) and is recommended,
        // when JDBC-Client and oracle-Server runs on the same machine.
        String driverType = "thin";
        String host = "localhost";
        String port = "1521";
        String sid = "freepdb1";
        String username = "hr";
        String pw = "oracle";

        String url = String.format("jdbc:oracle:%s:%s/%s@%s:%s/%s", driverType, username, pw, host, port, sid);

        try (Connection connection = DriverManager.getConnection(url)) {
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
