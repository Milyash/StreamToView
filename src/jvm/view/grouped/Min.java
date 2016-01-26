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
public class Min extends Aggregate {
    private static Logger LOG = LoggerFactory.getLogger(Min.class);

    public ViewField MIN_ROW_PK;
    public ViewField MIN;


    public Min() {
        super();
        initFields();
    }

    public Min(ViewField field) {
        super(field);
        initFields();
    }

    public Min(ViewField field, ViewField groupBy) {
        super(field, groupBy);
        LOG.error(LOG_STRING + super.toString());

        initFields();
    }

    private void initFields() {
        VIEW_TYPE = "minGroupBy";
        VIEW_ID = getId();
        VIEW_TABLE_NAME = "vt_" + getId();
        LOG_STRING = "====== Miv GROUP BY: ";
        MIN_ROW_PK = new ViewField(VIEW_ID, Const.AGGREGATE_VIEW_TABLE_COLUMN_PK, Const.AGGREGATE_VIEW_TABLE_COLUMN_FAMILY, Value.TYPE.STRING);
        MIN = new ViewField(VIEW_ID, Const.MIN_VIEW_TABLE_COLUMN_MIN, Const.AGGREGATE_VIEW_TABLE_COLUMN_FAMILY, Value.TYPE.INTEGER);

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
                    String currentMinPk = getCurrentMinPk(null);
                    if (pk.equals(currentMinPk))
                        selectionUpdates.add(updateMin(baseTable, null));
                    else
                        selectionUpdates.add(setMin(baseTable, updateField, pk, null));
                } else if (update instanceof TableRowDeleteUpd) {
                    if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) {
                        String currentMinPk = getCurrentMinPk(null);
                        if (pk.equals(currentMinPk)) {
                            deleteViewValueByAggrKey(null);
                            TableRowUpd selectionUpdate = updateMin(baseTable, null);
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

                        selectionUpdates.add(setMin(baseTable, updateField, pk, updateAggrKey));

                    }
                } else { // prevAggrKey!=null
                    if (!isFieldUpdated(update, groupBy, prevAggrKey)) {
                        if (isFieldUpdated(update, field, prevField)) {
                            String currentMinPk = getCurrentMinPk(prevAggrKey);
                            if (pk.equals(currentMinPk))
                                selectionUpdates.add(updateMin(baseTable, prevAggrKey));
                            else
                                selectionUpdates.add(setMin(baseTable, updateField, pk, prevAggrKey));
                        } else {
                            if (updateField != null) {
                                String currentMinPk = getCurrentMinPk(prevAggrKey);
                                if (currentMinPk == null) {
                                    selectionUpdates.add(setMin(baseTable, prevField, pk, prevAggrKey));
                                }
                                return null;
                            }
                        }
                    } else { //aggrKey is updated
                        selectionUpdates.add(updateMin(baseTable, prevAggrKey));

                        if (!isFieldUpdated(update, field, prevField))
                            updateField = prevField;
                        if (updateField == null) return null;

                        selectionUpdates.add(setMin(baseTable, updateField, pk, updateAggrKey));
                    }
                }
            } else if (update instanceof TableRowDeleteUpd) {
                if (update.areViewFieldsUpdated(Lists.newArrayList(field, groupBy))) {
                    String currentMinPk = getCurrentMinPk(prevAggrKey);
                    if (pk.equals(currentMinPk)) {
                        deleteViewValueByAggrKey(prevAggrKey);
                        selectionUpdates.add(new TableRowDeleteUpd(baseTable, pk, FIELD_TO_SELECTION));

                        selectionUpdates.add(updateMin(baseTable, prevAggrKey));
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


    private HashMap<ViewField, Object> getCurrentMin(String aggrKey) throws IOException {
        HashMap<ViewField, Object> currentMin = conn.getFieldsByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, Lists.newArrayList(MIN, MIN_ROW_PK));
        return currentMin;
    }

    private String getCurrentMinPk(String aggrKey) throws IOException {
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        HashMap<ViewField, Object> currentMinRow = getCurrentMin(aggrKey);
        if (currentMinRow.size() == 0) return null;
        String currentMinPk = (String) currentMinRow.get(MIN_ROW_PK);
        return currentMinPk;
    }

    private TableRowUpd updateMin(String tableName, String aggrKey) throws IOException {
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        HashMap<ViewField, Object> newMinRow = scanNewMin(aggrKey);
        if (newMinRow == null) {
            deleteViewValueByAggrKey(aggrKey);
            return new TableRowDeleteUpd(tableName, aggrKey, FIELD_TO_SELECTION);
        }
        String newMinPk = (String) newMinRow.get(MIN_ROW_PK);
        Integer newMin = (Integer) newMinRow.get(MIN);
        deleteViewValueByAggrKey(aggrKey);
        if (newMin != null) return setMin(tableName, newMin, newMinPk, aggrKey);
        else return new TableRowDeleteUpd(tableName, aggrKey, FIELD_TO_SELECTION);
    }

    private TableRowUpd setMin(String tableName, Integer newValue, String pk, String aggrKey) throws IOException {
        if (newValue == null) return null;
        if (aggrKey == null) aggrKey = Const.AGGREGATE_KEY;
        HashMap<ViewField, Object> currentMinRow = getCurrentMin(aggrKey);
        Integer currentMin = (Integer) currentMinRow.get(MIN);
        String currentMinPk = (String) currentMinRow.get(MIN_ROW_PK);

        if (pk.equals(currentMinPk) || currentMin == null || currentMin > newValue) {
            HashMap<ViewField, Object> newMinRow = new HashMap<>();
            newMinRow.put(MIN, newValue);
            newMinRow.put(MIN_ROW_PK, pk);
            conn.putFieldsByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKey, newMinRow);

            return new TableRowPutUpd(tableName, aggrKey, FIELD_TO_SELECTION, newValue);
        }

        return null;
    }

    private HashMap<ViewField, Object> scanNewMin(String aggrKey) throws IOException {

        HashMap<String, HashMap<ViewField, Object>> rows = conn.scanTableFields(HISTORY_TABLE_NAME, Lists.newArrayList(HISTORY_PREV_VERSION_FIELD,
                HISTORY_PREV_VERSION_AGGREGATE));
        if (!rows.isEmpty()) {
            String newMinPk = null;
            Integer newMin = null;
            for (Map.Entry<String, HashMap<ViewField, Object>> row : rows.entrySet()) {
                HashMap<ViewField, Object> rowFields = row.getValue();
                if (aggrKey.equals(Const.AGGREGATE_KEY) || aggrKey.equals(rowFields.get(HISTORY_PREV_VERSION_AGGREGATE))) {
                    Integer fieldValue = (Integer) rowFields.get(HISTORY_PREV_VERSION_FIELD);
                    if (fieldValue == null) continue;
                    if (newMin == null || fieldValue < newMin) {
                        newMin = fieldValue;
                        newMinPk = row.getKey();
                    }
                }
            }
            if (newMin != null) {
                HashMap<ViewField, Object> newMinRow = new HashMap<>();
                newMinRow.put(MIN, newMin);
                newMinRow.put(MIN_ROW_PK, newMinPk);
                return newMinRow;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "Min{" + super.toString() + "}";
    }
}
