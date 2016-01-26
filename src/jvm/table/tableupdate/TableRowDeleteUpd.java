package table.tableupdate;

import view.ViewField;

import java.util.ArrayList;

/**
 * Created by milya on 20.12.15.
 */
public class TableRowDeleteUpd extends TableRowUpd {
    public TableRowDeleteUpd(String tableName) {
        super(tableName);
    }

    public TableRowDeleteUpd(String tableName, String pk) {
        super(tableName, pk);
    }

    public TableRowDeleteUpd(String tableName, String pk, ArrayList<CellUpd> cellUpdates) {
        super(tableName, pk, cellUpdates);
    }

    public TableRowDeleteUpd(String tableName, String pk, ViewField field) {
        super(tableName, pk, field, null);
    }

    @Override
    public String toString() {
        return "TableRowDeleteUpd{" + super.toString() + "}";
    }
}
