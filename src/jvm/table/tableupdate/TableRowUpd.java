package table.tableupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.rowupdate.CellUpd;
import table.tableupdate.rowupdate.DBRow;
import table.value.ValueFabric;
import view.ViewField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by milya on 17.12.15.
 */
public class TableRowUpd implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(TableRowUpd.class);
    private String tableName;
    private DBRow row;

    public TableRowUpd(String tableName) {
        this.tableName = tableName;
    }

    public TableRowUpd(String tableName, DBRow row) {
        this.tableName = tableName;
        this.row = row;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DBRow getRow() {
        return row;
    }

    public void setRow(DBRow rows) {
        this.row = rows;
    }

    public String getPk() {
        return row.getPk();
    }

    public void setPk(String pk) {
        row.setPk(pk);
    }

    public ArrayList<CellUpd> getCellUpdates() {
        return row.getCellUpdates();
    }

    public void setCellUpdates(ArrayList<CellUpd> cellUpdates) {
        row.setCellUpdates(cellUpdates);
    }

    public void addCellUpdate(CellUpd cellUpd) {
        if (cellUpd == null) return;
        if (row.getCellUpdates() == null) row.setCellUpdates(new ArrayList<CellUpd>());
        row.addCellUpdate(cellUpd);
    }

    public void addCellUpdates(List<CellUpd> cellUpds) {
        row.addCellUpdates(cellUpds);
    }

    public ArrayList<ViewField> getUpdatedFields() {
        return row.getUpdatedFields();
    }

    public ArrayList<ViewField> getUnupdatedViewFields(ArrayList<ViewField> viewFields) {
        return row.getUnUpdatedFieldsFromList(viewFields);
    }

    public ArrayList<ViewField> getUpdatedFieldsFromList(ArrayList<ViewField> viewFields) {
        return row.getUpdatedFieldsFromList(viewFields);
    }

    public boolean areFieldsFromListUpdated(ViewField viewField) {
        return row.areFieldsFromListUpdated(viewField);
    }

    public Object getUpdatedValueByField(ViewField field) {
        for (ViewField viewField : row.getUpdatedFields()) {

            if (field.equals(viewField))
                return ValueFabric.getValue(row.getUpdatedValueByField(field), field.getDataType());
        }
        return null;
    }

    public boolean areUpdatedFieldsInList(ArrayList<ViewField> viewFields) {
        return getUpdatedFieldsFromList(viewFields).size() != 0;// if updated are not used in view
    }

    public ArrayList<CellUpd> getUpdatedViewCellsUpdates(ArrayList<ViewField> viewFields) {
        ArrayList<ViewField> updatedFields = getUpdatedFields();
        updatedFields.retainAll(viewFields); // updated fields and fields - intersection

//        LOG.error("****************** TableRowUpd getUpdatedViewCellsUpdates : " + getUpdatedFields() + "; retain: " + viewFields + "; result: " + updatedFields);

        ArrayList<CellUpd> result = new ArrayList<>();
        for (CellUpd cellUpd : row.getCellUpdates()) {
            if (updatedFields.contains(cellUpd.getField()))
                result.add(cellUpd);
        }
        return result;
    }

    @Override
    public String toString() {
        return "TableRowUpd{" +
                "tableName='" + tableName + '\'' +
                ", row=" + row +
                '}';
    }
}
