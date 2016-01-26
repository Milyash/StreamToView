package view.grouped;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.TableRowDeleteUpd;
import table.tableupdate.TableRowPutUpd;
import table.tableupdate.TableRowUpd;
import table.value.Value;
import view.Const;
import view.ViewField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by milya on 07.01.16.
 */
public class Sum extends Aggregate {
    private static Logger LOG = LoggerFactory.getLogger(Sum.class);

    public ViewField SUM;

    public Sum() {
        super();
        initFields();
    }

    public Sum(ViewField field) {
        super(field);
        initFields();
    }

    public Sum(ViewField field, ViewField groupBy) {
        super(field, groupBy);
        LOG.error(LOG_STRING + super.toString());

        initFields();
    }

    private void initFields() {
        VIEW_TYPE = "sumGroupBy";
        VIEW_ID = getId();
        LOG_STRING = "====== Sum GROUP BY: ";
        SUM = new ViewField(VIEW_ID, Const.SUM_VIEW_TABLE_COLUMN, Const.AGGREGATE_VIEW_TABLE_COLUMN_FAMILY, Value.TYPE.INTEGER);
        FIELD_TO_SELECTION = new ViewField(field.getTableName(), VIEW_TYPE + field.getColumnName(), field.getFamilyName(), Value.TYPE.INTEGER);
    }

    @Override
    public ArrayList<TableRowUpd> processAggregateUpdate(TableRowUpd update, String viewName, HashMap<ViewField, Object> historyEntry) {

        LOG.error(LOG_STRING + " got update: " + update);

        ArrayList<TableRowUpd> selectionUpdates = new ArrayList<>();
        try {

            if (!update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) return null;

            String updateTable = update.getTableName();
            Integer updateField = (Integer) update.getUpdatedValueByField(field);

            Integer prevField = (Integer) historyEntry.get(HISTORY_PREV_VERSION_FIELD);
            String prevAggrKey = (String) historyEntry.get(HISTORY_PREV_VERSION_AGGREGATE);

            if (groupBy == null) {
                if (update instanceof TableRowPutUpd) {
                    updateHistoryTable(update);
                    if (updateField == null) updateField = 0;
                    selectionUpdates.add(addSum(updateTable, (updateField - prevField), null));
                } else if (update instanceof TableRowDeleteUpd) {
                    updateHistoryTable(update);
                    if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy)))
                        selectionUpdates.add(substrSum(updateTable, prevField, null));
                }
                if (!selectionUpdates.isEmpty()) return selectionUpdates;
                return null;
            }

            String updateAggrKey = prevAggrKey;
            if (update.areViewFieldsUpdated(groupBy))
                if (update instanceof TableRowDeleteUpd)
                    updateAggrKey = null;
                else
                    updateAggrKey = (String) update.getUpdatedValueByField(groupBy);

            boolean groupByUpdated = isFieldUpdated(update, groupBy, prevAggrKey);
            boolean fieldUpdated = isFieldUpdated(update, field, prevField);


            if (update instanceof TableRowPutUpd) {
                if (prevAggrKey == null) {
                    if (!groupByUpdated) return null;
                    else {
                        updateHistoryTable(update);
                        if (!fieldUpdated)
                            updateField = prevField;
                        if (updateField == null) return null;

                        selectionUpdates.add(addSum(updateTable, updateField, updateAggrKey));
                    }
                } else { // prevAggrKey!=null
                    if (!groupByUpdated)
                        if (fieldUpdated) {
                            updateHistoryTable(update);
                            if (updateField == null) updateField = 0;
                            selectionUpdates.add(addSum(updateTable, (updateField - prevField), updateAggrKey));

                    } else {
                        updateHistoryTable(update);
                        if (isAggrKeyInHistTable(prevAggrKey))
                            selectionUpdates.add(substrSum(updateTable, prevField, prevAggrKey));
                        else {
                            deleteViewValueByAggrKey(prevAggrKey);
                            selectionUpdates.add(new TableRowDeleteUpd(updateTable, prevAggrKey, FIELD_TO_SELECTION));
                        }
                        if (!fieldUpdated)
                            updateField = prevField;
                        if (updateField != null)
                            selectionUpdates.add(addSum(updateTable, updateField, updateAggrKey));
                    }
                }
            } else if (update instanceof TableRowDeleteUpd) {
                updateHistoryTable(update);
                if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) {
                    selectionUpdates.add(substrSum(updateTable, prevField, prevAggrKey));
                }
            }
            if (!selectionUpdates.isEmpty()) return selectionUpdates;
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    private Integer getCurrentSum(String aggrKey) throws IOException {
        return (Integer) conn.getFieldByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, SUM);
    }

    private boolean isAggrKeyInHistTable(String aggrKey) throws IOException {

        ArrayList<String> keys = conn.scanTableField(HISTORY_TABLE_NAME, HISTORY_PREV_VERSION_AGGREGATE);
        for (String key : keys)
            if (aggrKey.equals(key)) return true;
        return false;
    }

    private TableRowUpd addSum(String tableName, Integer newValue, String aggrKey) throws IOException {
        if (newValue == null) return null;
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        Integer currentSum = getCurrentSum(aggrKey);
        if (currentSum == null) currentSum = 0;

        Integer newSum = currentSum + newValue;
        HashMap<ViewField, Object> newSumRow = new HashMap<>();
        newSumRow.put(SUM, newSum);
        conn.putFieldsByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, newSumRow);
        return new TableRowPutUpd(tableName, aggrKey, FIELD_TO_SELECTION, newSumRow);
    }

    private TableRowUpd substrSum(String tableName, Integer newValue, String aggrKey) throws IOException {
        if (newValue == null) return null;
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        Integer currentSum = getCurrentSum(aggrKey);
        if (currentSum == null) return null;

        Integer newSum = currentSum - newValue;
        HashMap<ViewField, Object> newSumRow = new HashMap<>();
        newSumRow.put(SUM, newSum);
        conn.putFieldsByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, newSumRow);
        return new TableRowPutUpd(tableName, aggrKey, FIELD_TO_SELECTION, newSumRow);
    }

    @Override
    public String toString() {
        return "Sum{" + super.toString() + "}";
    }
}