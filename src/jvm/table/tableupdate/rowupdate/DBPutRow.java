package table.tableupdate.rowupdate;

import java.util.ArrayList;

/**
 * Created by milya on 17.12.15.
 */
public class DBPutRow extends DBRow {
    public DBPutRow() {
    }

    public DBPutRow(String pk) {
        super(pk);
    }

    public DBPutRow(String pk, ArrayList<CellUpd> cellUpdates) {
        super(pk, cellUpdates);
    }

    @Override
    public String toString() {
        return "DBPutRow{" + super.toString() + "}";
    }
}
