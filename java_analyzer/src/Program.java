import java.util.List;

public class Program {
    public static void main(String[] args) {
        DbAnalyzer dbAnalyzer = new DbAnalyzer();
        List<TableColumnRowCounts> tableStatistics =  dbAnalyzer.getTableStatistics();
        printTableStatistics(tableStatistics);
    }
    
    public static void printTableStatistics(List<TableColumnRowCounts> tables) {
        System.out.println("Table name, number of columns, number of rows");
        for (TableColumnRowCounts row : tables) {
            System.out.println(row.getTableName() + " " + row.getColumnCount() + " " + row.getRowCount());
        }
    }
}
