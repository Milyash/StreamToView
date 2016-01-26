package table.tableupdate;

import view.ViewField;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by milya on 20.12.15.
 */
public class TableRowPutUpd extends TableRowUpd {

    public TableRowPutUpd(String tableName) {
        super(tableName);
    }

    public TableRowPutUpd(String tableName, String pk) {
        super(tableName, pk);
    }

    public TableRowPutUpd(String tableName, String pk, ArrayList<CellUpd> cellUpdates) {
        super(tableName, pk, cellUpdates);
    }

    public TableRowPutUpd(String tableName, String pk, HashMap<ViewField, Object> fieldUpdates) {
        super(tableName, pk, fieldUpdates);
    }

    public TableRowPutUpd(String tableName, String pk, ViewField field, Object value) {
        super(tableName, pk, field, value);
    }

    @Override
    public String toString() {
        return "TableRowPutUpd{" + super.toString() + "}";
    }
}
