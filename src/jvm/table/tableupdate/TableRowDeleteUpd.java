package table.tableupdate;

import table.tableupdate.rowupdate.DBDeleteRow;

/**
 * Created by milya on 20.12.15.
 */
public class TableRowDeleteUpd extends TableRowUpd {
    public TableRowDeleteUpd(String tableName) {
        super(tableName);
    }

    public TableRowDeleteUpd(String tableName, DBDeleteRow row) {
        super(tableName, row);
    }

    @Override
    public String toString() {
        return "TableRowDeleteUpd{" + super.toString() + "}";
    }
}
