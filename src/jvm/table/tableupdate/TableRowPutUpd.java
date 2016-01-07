package table.tableupdate;

import table.tableupdate.rowupdate.DBPutRow;

/**
 * Created by milya on 20.12.15.
 */
public class TableRowPutUpd extends TableRowUpd {

    public TableRowPutUpd(String tableName) {
        super(tableName);
    }

    public TableRowPutUpd(String tableName, DBPutRow row) {
        super(tableName, row);
    }

    @Override
    public String toString() {
        return "TableRowPutUpd{" + super.toString() + "}";
    }
}
