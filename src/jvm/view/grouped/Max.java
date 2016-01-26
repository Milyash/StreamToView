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
import java.util.Map;

/**
 * Created by milya on 07.01.16.
 */
public class Max extends Aggregate {
    private static Logger LOG = LoggerFactory.getLogger(Max.class);

    public ViewField MAX_ROW_PK;
    public ViewField MAX;

    public Max() {
        super();
        initFields();
    }

    public Max(ViewField field) {
        super(field);
        initFields();
    }

    public Max(ViewField field, ViewField groupBy) {
        super(field, groupBy);
        LOG.error(LOG_STRING + super.toString());
        initFields();
    }

    private void initFields() {
        LOG_STRING = "====== Max GROUP BY: ";
        VIEW_TYPE = "maxGroupBy";
        VIEW_ID = getId();
        VIEW_TABLE_NAME = "vt_" + VIEW_ID;
        MAX_ROW_PK = new ViewField(VIEW_ID, Const.AGGREGATE_VIEW_TABLE_COLUMN_PK, Const.AGGREGATE_VIEW_TABLE_COLUMN_FAMILY, Value.TYPE.STRING);
        MAX = new ViewField(VIEW_ID, Const.MAX_VIEW_TABLE_COLUMN_MAX, Const.AGGREGATE_VIEW_TABLE_COLUMN_FAMILY, Value.TYPE.INTEGER);

        FIELD_TO_SELECTION = new ViewField(field.getTableName(), VIEW_TYPE + field.getColumnName(), field.getFamilyName(), Value.TYPE.INTEGER);
    }


    @Override
    public ArrayList<TableRowUpd> processAggregateUpdate(TableRowUpd update, String viewName, HashMap<ViewField, Object> historyEntry) {

        LOG.error(LOG_STRING + " got update: " + update);

        ArrayList<TableRowUpd> selectionUpdates = new ArrayList<>();
        try {

            if (!update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) return null;

            String pk = update.getPk();
            String baseTable = update.getTableName();
            Integer updateField = (Integer) update.getUpdatedValueByField(field);

            Integer prevField = (Integer) historyEntry.get(HISTORY_PREV_VERSION_FIELD);
            String prevAggrKey = (String) historyEntry.get(HISTORY_PREV_VERSION_AGGREGATE);


            if (groupBy == null) {
                if (update instanceof TableRowPutUpd) {
                    String currentMaxPk = getCurrentMaxPk(null);//  updateAggrKey=prevAggrKey
                    if (pk.equals(currentMaxPk))
                        selectionUpdates.add(updateMax(baseTable, null));
                    else
                        selectionUpdates.add(setMax(baseTable, updateField, pk, null));

                } else if (update instanceof TableRowDeleteUpd) {
                    if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) {
                        String currentMaxPk = getCurrentMaxPk(null);
                        if (pk.equals(currentMaxPk)) {
                            deleteViewValueByAggrKey(null);
                            TableRowUpd selectionUpdate = updateMax(baseTable, null);
                            if (selectionUpdate != null)
                                selectionUpdates.add(selectionUpdate);
                            else
                                selectionUpdates.add(new TableRowDeleteUpd(baseTable, pk, FIELD_TO_SELECTION));
                        }
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
                        if (!isFieldUpdated(update, field, prevField))
                            updateField = prevField;
                        if (updateField == null) return null;
                        selectionUpdates.add(setMax(baseTable, updateField, pk, updateAggrKey));
                    }
                } else { // prevAggrKey!=null
                    if (!isFieldUpdated(update, groupBy, prevAggrKey)) {
                        if (isFieldUpdated(update, field, prevField)) {
                            String currentMaxPk = getCurrentMaxPk(prevAggrKey);
                            if (pk.equals(currentMaxPk))
                                selectionUpdates.add(updateMax(baseTable, prevAggrKey));
                            else
                                selectionUpdates.add(setMax(baseTable, updateField, pk, prevAggrKey));
                        } else {
                            if (updateField != null) { // prevField == null
                                String currentMaxPk = getCurrentMaxPk(prevAggrKey);
                                if (currentMaxPk == null) {
                                    selectionUpdates.add(setMax(baseTable, prevField, pk, prevAggrKey));
                                }
                                return null;
                            }
                        }
                    } else { //aggrKey is updated
                        selectionUpdates.add(updateMax(baseTable, prevAggrKey));

                        if (!isFieldUpdated(update, field, prevField))
                            updateField = prevField;
                        if (updateField == null) return null;

                        selectionUpdates.add(setMax(baseTable, updateField, pk, updateAggrKey));
                    }
                }
            } else if (update instanceof TableRowDeleteUpd) {
                if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) {
                    String currentMaxPk = getCurrentMaxPk(prevAggrKey);
                    if (pk.equals(currentMaxPk)) {
                        deleteViewValueByAggrKey(prevAggrKey);
                        selectionUpdates.add(new TableRowDeleteUpd(baseTable, prevAggrKey, FIELD_TO_SELECTION));
                        selectionUpdates.add(updateMax(baseTable, prevAggrKey));
                    }
                }
            }

            if (!selectionUpdates.isEmpty()) return selectionUpdates;
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private HashMap<ViewField, Object> getCurrentMax(String aggrKeyValue) throws IOException {
        HashMap<ViewField, Object> currentMax = conn.getFieldsByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKeyValue, Lists.newArrayList(MAX, MAX_ROW_PK));
        return currentMax;
    }

    private String getCurrentMaxPk(String aggrKey) throws IOException {
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        HashMap<ViewField, Object> currentMaxRow = getCurrentMax(aggrKey);
        if (currentMaxRow.size() == 0) return null;
        String currentMaxPk = (String) currentMaxRow.get(MAX_ROW_PK);
        return currentMaxPk;
    }

    private TableRowUpd updateMax(String tableName, String aggrKey) throws IOException {
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        HashMap<ViewField, Object> newMaxRow = scanNewMax(aggrKey);
        if (newMaxRow == null) {
            deleteViewValueByAggrKey(aggrKey);
            return new TableRowDeleteUpd(tableName, aggrKey, FIELD_TO_SELECTION);
        }
        String newMaxPk = (String) newMaxRow.get(MAX_ROW_PK);
        Integer newMax = (Integer) newMaxRow.get(MAX);
        deleteViewValueByAggrKey(aggrKey);
        if (newMax != null) {
            return setMax(tableName, newMax, newMaxPk, aggrKey);
        } else return new TableRowDeleteUpd(tableName, aggrKey, FIELD_TO_SELECTION);
    }

    private TableRowUpd setMax(String tableName, Integer newValue, String pk, String aggrKey) throws IOException {
        if (newValue == null) return null;
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        HashMap<ViewField, Object> currentMaxRow = getCurrentMax(aggrKey);
        Integer currentMax = (Integer) currentMaxRow.get(MAX);
        String currentMaxPk = (String) currentMaxRow.get(MAX_ROW_PK);

        if (pk.equals(currentMaxPk) || currentMax == null || currentMax < newValue) {
            HashMap<ViewField, Object> newMaxRow = new HashMap<>();
            newMaxRow.put(MAX, newValue);
            newMaxRow.put(MAX_ROW_PK, pk);
            conn.putFieldsByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, newMaxRow);

            return new TableRowPutUpd(tableName, aggrKey, FIELD_TO_SELECTION, newValue);
        }
        return null;
    }

    private HashMap<ViewField, Object> scanNewMax(String aggrKey) throws IOException {

        HashMap<String, HashMap<ViewField, Object>> rows = conn.scanTableFields(HISTORY_TABLE_NAME, Lists.newArrayList(HISTORY_PREV_VERSION_FIELD,
                HISTORY_PREV_VERSION_AGGREGATE));
        if (!rows.isEmpty()) {
            String newMaxPk = null;
            Integer newMax = null;
            for (Map.Entry<String, HashMap<ViewField, Object>> row : rows.entrySet()) {
                HashMap<ViewField, Object> rowFields = row.getValue();
                if (aggrKey.equals(Const.AGGREGATE_KEY) || aggrKey.equals(rowFields.get(HISTORY_PREV_VERSION_AGGREGATE))) {
                    Integer fieldValue = (Integer) rowFields.get(HISTORY_PREV_VERSION_FIELD);
                    if (fieldValue == null) continue;
                    if (newMax == null || fieldValue > newMax) {
                        newMax = fieldValue;
                        newMaxPk = row.getKey();
                    }
                }
            }
            if (newMax != null) {
                HashMap<ViewField, Object> newMaxRow = new HashMap<>();
                newMaxRow.put(MAX, newMax);
                newMaxRow.put(MAX_ROW_PK, newMaxPk);
                return newMaxRow;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "Max{" + super.toString() + "}";
    }
}
