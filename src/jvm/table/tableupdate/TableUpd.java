package table.tableupdate;

import table.tableupdate.rowupdate.DBDeleteRow;
import table.tableupdate.rowupdate.DBPutRow;
import table.tableupdate.rowupdate.DBRow;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by milya on 17.12.15.
 */
public class TableUpd implements Serializable {
    private String tableName;
    private ArrayList<DBRow> rows;

    public TableUpd(String tableName) {
        this.tableName = tableName;
    }

    public TableUpd(String tableName, ArrayList<DBRow> rows) {
        this.tableName = tableName;
        this.rows = rows;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ArrayList<DBRow> getRows() {
        return rows;
    }

    public void setRows(ArrayList<DBRow> rows) {
        this.rows = rows;
    }

    public void addRow(DBRow row) {
        this.rows.add(row);
    }

    public ArrayList<TableRowUpd> getTableRowUpdates() {
        ArrayList<TableRowUpd> rowsUpdates = new ArrayList<>();
        for (DBRow row : rows) {
            if (row instanceof DBPutRow)
                rowsUpdates.add(new TableRowPutUpd(tableName, (DBPutRow) row));
            else if (row instanceof DBDeleteRow)
                rowsUpdates.add(new TableRowDeleteUpd(tableName, (DBDeleteRow) row));
        }

        return rowsUpdates;
    }

    @Override
    public String toString() {
        return "TableUpd{" +
                "tableName='" + tableName + '\'' +
                ", rows=" + rows +
                '}';
    }
}
