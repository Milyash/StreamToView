package bolt;

import db.DBConnector;
import table.tableupdate.CellUpd;
import table.tableupdate.TableRowPutUpd;
import table.tableupdate.TableRowUpd;
import view.Const;
import view.ViewField;
import view.grouped.Aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by milya on 26.01.16.
 */
public class HistoryTableController {
    DBConnector conn;

    public HistoryTableController() {
        this.conn = new DBConnector();
    }

    public HashMap<ViewField, Object> getHistoryRow(ArrayList<Aggregate> aggregates, String pk, String tableName) {
        HashMap<ViewField, Object> historyEntry = new HashMap<>();
        try {

            String historyTableName = Const.HISTORY_TABLE_NAME_PREFIX + tableName;
            ArrayList<ViewField> aggregateFields = new ArrayList<>();
            for (Aggregate aggregate : aggregates) {
                ViewField tableField = aggregate.getField();
                ViewField historyTableField = new ViewField(historyTableName, tableField.getColumnName(), tableField.getFamilyName(), tableField.getDataType());
                if (!aggregateFields.contains(historyTableField))
                    aggregateFields.add(historyTableField);

                ViewField tableGroupByField = aggregate.getGroupBy();
                if (tableGroupByField == null) continue;
                ViewField historyTableGoupByField = new ViewField(historyTableName, tableGroupByField.getColumnName(), tableGroupByField.getFamilyName(), tableGroupByField.getDataType());
                if (!aggregateFields.contains(historyTableGoupByField))
                    aggregateFields.add(historyTableGoupByField);
            }

            HashMap<ViewField, Object> row = null;
            row = conn.getFieldsByPk(historyTableName, pk, aggregateFields);

            if (row.size() != 0) {
                for (ViewField field : aggregateFields)
                    historyEntry.put(field, row.get(field));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return historyEntry;
    }

    public void updateHistoryTable(TableRowUpd update) {
        try {
            String pk = update.getPk();
            String historyTableName = Const.HISTORY_TABLE_NAME_PREFIX + update.getTableName();
            if (update instanceof TableRowPutUpd) {
                HashMap<ViewField, byte[]> values = new HashMap<>();
                for (CellUpd cellUpd : update.getCellUpdates())
                    values.put(cellUpd.getField(), cellUpd.getValue());

                conn.putByteFieldsByPk(historyTableName, pk, values);

            } else
                conn.deleteFieldsByPk(historyTableName, pk, update.getUpdatedFields());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
