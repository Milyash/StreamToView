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
public class Count extends Aggregate {
    private static Logger LOG = LoggerFactory.getLogger(Count.class);

    public ViewField COUNT;

    public Count() {
        super();
        initFields();
    }

    public Count(ViewField field) {
        super(field);
        initFields();
    }

    public Count(ViewField field, ViewField groupBy) {
        super(field, groupBy);

        LOG.error(LOG_STRING + super.toString());

        initFields();
    }

    private void initFields() {
        VIEW_TYPE = "countGroupBy";
        VIEW_ID = getId();
        LOG_STRING = "====== Count GROUP BY: ";
        COUNT = new ViewField(VIEW_ID, Const.COUNT_VIEW_TABLE_COLUMN, Const.AGGREGATE_VIEW_TABLE_COLUMN_FAMILY, Value.TYPE.INTEGER);
        FIELD_TO_SELECTION = new ViewField(field.getTableName(), VIEW_TYPE + field.getColumnName(), field.getFamilyName(), Value.TYPE.INTEGER);
    }

    @Override
    public ArrayList<TableRowUpd> processAggregateUpdate(TableRowUpd update, String viewName, HashMap<ViewField, Object> historyEntry) {

        LOG.error(LOG_STRING + " got update: " + update);
        ArrayList<TableRowUpd> selectionUpdates = new ArrayList<>();

        try {

            if (!update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) return null;

            String updatePk = update.getPk();
            String updateTable = update.getTableName();
            Integer updateField = (Integer) update.getUpdatedValueByField(field);

            Integer prevField = (Integer) historyEntry.get(HISTORY_PREV_VERSION_FIELD);
            String prevAggrKey = (String) historyEntry.get(HISTORY_PREV_VERSION_AGGREGATE);


            if (groupBy == null) {
                if (update instanceof TableRowPutUpd) {
                    updateHistoryTable(update);

                    if (updateField == null) {  // prevField != null
                        selectionUpdates.add(substrCount(updateTable, null));
                    } else if (prevField == null) { // newField != null
                        selectionUpdates.add(addCount(updateTable, null));
                    }
                } else if (update instanceof TableRowDeleteUpd) {
                    updateHistoryTable(update);
                    if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) {
                        selectionUpdates.add(substrCount(updateTable, null));
                    }
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


            if (update instanceof TableRowPutUpd) {
                if (prevAggrKey == null) {
                    if (!isFieldUpdated(update, groupBy, prevAggrKey)) return null;
                    else {
                        updateHistoryTable(update);

                        if (!isFieldUpdated(update, field, prevField))
                            updateField = prevField;
                        if (updateField == null) return null;

                        selectionUpdates.add(addCount(updateTable, updateAggrKey));
                    }
                } else { // prevAggrKey!=null
                    if (!isFieldUpdated(update, groupBy, prevAggrKey)) {
                        if (isFieldUpdated(update, field, prevField)) {
                            updateHistoryTable(update);
                            if (updateField == null)
                                selectionUpdates.add(substrCount(updateTable, updateAggrKey));
                            else if (prevField == null)
                                selectionUpdates.add(addCount(updateTable, updateAggrKey));
                        }
                    } else { //aggrKey is updated
                        updateHistoryTable(update);
                        if (prevField != null)
                            selectionUpdates.add(substrCount(updateTable, prevAggrKey));

                        if (!isFieldUpdated(update, field, prevField))
                            updateField = prevField;
                        if (updateField != null)
                            selectionUpdates.add(addCount(updateTable, updateAggrKey));
                    }
                }
            } else if (update instanceof TableRowDeleteUpd) {
                updateHistoryTable(update);
                if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) {
                    selectionUpdates.add(substrCount(updateTable, prevAggrKey));
                }

            }
            if (!selectionUpdates.isEmpty()) return selectionUpdates;
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    private TableRowUpd addCount(String tableName, String aggrKey) throws IOException {
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        Integer curCount = (Integer) conn.getFieldByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, COUNT);
        if (curCount == null) curCount = 0;
        curCount++;
        conn.putFieldByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, COUNT, curCount);
        return new TableRowPutUpd(tableName, aggrKey, FIELD_TO_SELECTION, curCount);
    }

    private TableRowUpd substrCount(String tableName, String aggrKey) throws IOException {
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        Integer curCount = (Integer) conn.getFieldByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, COUNT);
        if (curCount == null) return null;
        curCount--;
        conn.putFieldByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, COUNT, curCount);
        return new TableRowPutUpd(tableName, aggrKey, FIELD_TO_SELECTION, curCount);
    }

    @Override
    public String toString() {
        return "Count{" + super.toString() + "}";
    }
}
