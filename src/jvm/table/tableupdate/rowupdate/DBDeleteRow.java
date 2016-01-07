package table.tableupdate.rowupdate;

import java.util.ArrayList;

/**
 * Created by milya on 17.12.15.
 */
public class DBDeleteRow extends DBRow {

    public DBDeleteRow() {
    }

    public DBDeleteRow(String pk) {
        super(pk);
    }

    public DBDeleteRow(String pk, ArrayList<CellUpd> cellUpdates) {
        super(pk, cellUpdates);
    }

    @Override
    public String toString() {
        return "DBDeleteRow{" + super.toString() + "}";
    }
}
