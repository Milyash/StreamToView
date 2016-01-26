package view.grouped;

import com.google.common.collect.Lists;
import table.tableupdate.TableRowDeleteUpd;
import table.tableupdate.TableRowPutUpd;
import table.tableupdate.TableRowUpd;
import view.Const;
import view.ViewField;
import view.ViewPart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by milya on 24.01.16.
 */
public class Aggregate extends ViewPart {
    protected ViewField field;
    protected ViewField groupBy;

    protected static String VIEW_ID;
    protected String VIEW_TABLE_NAME;
    public String VIEW_TYPE;

    public String HISTORY_TABLE_NAME;
    protected ViewField HISTORY_PREV_VERSION_AGGREGATE;
    protected ViewField HISTORY_PREV_VERSION_FIELD;
    public ViewField FIELD_TO_SELECTION;

    protected static String LOG_STRING;

    public Aggregate() {
        super();
    }

    public Aggregate(ViewField field) {
        this.field = field;
    }

    public Aggregate(ViewField field, ViewField groupBy) {
        this.field = field;
        this.groupBy = groupBy;

        HISTORY_TABLE_NAME = Const.HISTORY_TABLE_NAME_PREFIX + field.getTableName();
        HISTORY_PREV_VERSION_FIELD = new ViewField(Const.HISTORY_TABLE_NAME_PREFIX + field.getTableName(), field.getColumnName(), field.getFamilyName(), field.getDataType());
        if (groupBy != null)
            HISTORY_PREV_VERSION_AGGREGATE = new ViewField(Const.HISTORY_TABLE_NAME_PREFIX + field.getTableName(), groupBy.getColumnName(), field.getFamilyName(), groupBy.getDataType());
    }

    public ViewField getField() {
        return field;
    }

    public void setField(ViewField field) {
        this.field = field;
    }

    public ViewField getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(ViewField groupBy) {
        this.groupBy = groupBy;
    }

    public static Aggregate createAggregate(ViewField field, ViewField groupBy, String type) {
        switch (type) {
            case "max":
                return new Max(field, groupBy);
            case "min":
                return new Min(field, groupBy);
            case "sum":
                return new Sum(field, groupBy);
            case "count":
                return new Count(field, groupBy);
        }
        return new Aggregate(field, groupBy);
    }

    @Override
    public boolean processUpdate(TableRowUpd update, String viewName) {
        return false;
    }

    public ArrayList<TableRowUpd> processAggregateUpdate(TableRowUpd update, String viewName, HashMap<ViewField, Object> historyEntry) {
        return null;
    }

    @Override
    public String getId() {

        StringBuilder sb = new StringBuilder(VIEW_TYPE + "-"
                + field.getTableName() + "-"
                + field.getFamilyName() + "-"
                + field.getColumnName());
        if (groupBy != null)
            sb.append("-groupBy-"
                    + groupBy.getTableName() + "-"
                    + groupBy.getFamilyName() + "-"
                    + groupBy.getColumnName());

        return sb.toString();

    }

    @Override
    public String toString() {
        String str = "field=" + field;
        if (groupBy != null)
            str += ", groupBy=" + groupBy;
        return str;
    }

    public String getGroupByString() {

        if (groupBy == null) return "";
        StringBuilder sb = new StringBuilder("-groupBy-"
                + groupBy.getTableName() + "-"
                + groupBy.getFamilyName() + "-"
                + groupBy.getColumnName());

        return sb.toString();

    }

    public void prepareTable(){
        try {
            if (!conn.tableExists(VIEW_TABLE_NAME))
                conn.createTable(VIEW_TABLE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected boolean isFieldUpdated(TableRowUpd update, ViewField field, Object fieldValue) {
        if (field == null) return false;
        if (update instanceof TableRowDeleteUpd) return update.areViewFieldsUpdated(field);
        if (fieldValue == null && update.getUpdatedValueByField(field) == null) return false;
        return (update.areViewFieldsUpdated(field) && (update.getUpdatedValueByField(field) == null || !update.getUpdatedValueByField(field).equals(fieldValue)));
    }


    protected void updateHistoryTable(TableRowUpd update) throws IOException {
        String pk = update.getPk();
        if (update instanceof TableRowPutUpd) {
            HashMap<ViewField, Object> values = new HashMap<>();
            if (update.areViewFieldsUpdated(field))
                values.put(HISTORY_PREV_VERSION_FIELD, update.getUpdatedValueByField(field));
            if (update.areViewFieldsUpdated(groupBy))
                values.put(HISTORY_PREV_VERSION_AGGREGATE, update.getUpdatedValueByField(groupBy));

            conn.putFieldsByPk(HISTORY_TABLE_NAME, pk, values);
        } else
            conn.deleteFieldsByPk(HISTORY_TABLE_NAME, pk, Lists.newArrayList(HISTORY_PREV_VERSION_FIELD, HISTORY_PREV_VERSION_AGGREGATE));
    }

    protected void deleteViewValueByAggrKey(String aggrKeyValue) throws IOException {
        if (aggrKeyValue == null) aggrKeyValue = "";
        conn.deleteRowByPk(VIEW_TABLE_NAME, VIEW_ID + aggrKeyValue);
    }
}
