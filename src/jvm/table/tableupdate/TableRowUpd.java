package table.tableupdate;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.value.ValueFabric;
import view.ViewField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by milya on 17.12.15.
 */
public class TableRowUpd implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(TableRowUpd.class);
    private String tableName;
    private String pk;
    private ArrayList<CellUpd> cellUpdates;

    public TableRowUpd(String tableName) {
        this.tableName = tableName;
        this.cellUpdates = new ArrayList<>();
    }

    public TableRowUpd(String tableName, String pk) {
        this.tableName = tableName;
        this.pk = pk;
        this.cellUpdates = new ArrayList<>();
    }

    public TableRowUpd(String tableName, String pk, ArrayList<CellUpd> cellUpdates) {
        this.tableName = tableName;
        this.pk = pk;
        this.cellUpdates = cellUpdates;
    }

    public TableRowUpd(String tableName, String pk, HashMap<ViewField, Object> fieldUpdates) {
        this.tableName = tableName;
        this.pk = pk;
        this.cellUpdates = new ArrayList<>();
        for (Map.Entry<ViewField, Object> fieldUpdate : fieldUpdates.entrySet()) {
            this.cellUpdates.add(new CellUpd(false, fieldUpdate.getKey(), fieldUpdate.getValue()));
        }
    }

    public TableRowUpd(String tableName, String pk, ViewField field, Object value) {
        this.tableName = tableName;
        this.pk = pk;
        this.cellUpdates = Lists.newArrayList(new CellUpd(false, field, value));
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public ArrayList<CellUpd> getCellUpdates() {
        return cellUpdates;
    }

    public void setCellUpdates(ArrayList<CellUpd> cellUpdates) {
        this.cellUpdates = cellUpdates;
    }

    public void addCellUpdate(CellUpd cellUpd) {
        addCellUpdates(Lists.newArrayList(cellUpd));
    }

    public void addCellUpdates(List<CellUpd> cellUpds) {
        if (cellUpds == null || cellUpds.size() == 0) return;
        if (cellUpdates == null) cellUpdates = new ArrayList<>();
        cellUpdates.addAll(cellUpds);
    }

    public ArrayList<ViewField> getUpdatedFields() {
        ArrayList<ViewField> fields = new ArrayList<>();
        for (CellUpd cellUpd : cellUpdates)
            fields.add(cellUpd.getField());
        return fields;
    }

    public Object getUpdatedValueByField(ViewField field) {
        byte[] byteValue = null;
        for (CellUpd cellUpd : cellUpdates) {
            if (cellUpd.getField().equals(field)) {
                byteValue = cellUpd.getValue();
                break;
            }
        }
        return ValueFabric.getValue(byteValue, field.getDataType());
    }

    public byte[] getUpdatedValueBytesByField(ViewField field) {
        for (CellUpd cellUpd : cellUpdates) {
            if (cellUpd.getField().equals(field)) {
                return cellUpd.getValue();
            }
        }
        return null;
    }


    public ArrayList<ViewField> getUpdatedViewFields(ArrayList<ViewField> viewFields) {
        return intersection(getUpdatedFields(), viewFields);
        //retain doesn't work because it keeps the order of elements
    }

    public ArrayList<ViewField> getUnUpdatedViewFields(ArrayList<ViewField> viewFields) {
        ArrayList<ViewField> fieldsCopy = (ArrayList<ViewField>) viewFields.clone();
        fieldsCopy.removeAll(getUpdatedFields());
        return fieldsCopy;
    }

    public boolean areViewFieldsUpdated(ViewField viewField) {
        ArrayList<ViewField> viewFields = Lists.newArrayList(viewField);
        return areViewFieldsUpdated(viewFields);
    }

    public boolean areViewFieldsUpdated(ArrayList<ViewField> viewFields) {
        if (this instanceof TableRowDeleteUpd && cellUpdates.isEmpty()) return true;
        return getUpdatedViewFields(viewFields).size() > 0;
    }

    public ArrayList<CellUpd> getUpdatedViewCells(ArrayList<ViewField> viewFields) {
        ArrayList<ViewField> updatedFields = getUpdatedViewFields(viewFields);

        ArrayList<CellUpd> result = new ArrayList<>();
        for (CellUpd cellUpd : cellUpdates) {
            if (updatedFields.contains(cellUpd.getField()))
                result.add(cellUpd);
        }
        return result;
    }

    @Override
    public String toString() {
        return "TableRowUpd{" +
                "tableName='" + tableName + '\'' +
                ", pk='" + pk + '\'' +
                ", cellUpdates=" + cellUpdates +
                '}';
    }

    private ArrayList<ViewField> intersection(ArrayList<ViewField> l1, ArrayList<ViewField> l2) {
        ArrayList<ViewField> intersection = new ArrayList<>();
        for (ViewField vf : l1)
            if (l2.contains(vf))
                intersection.add(vf);
        return intersection;
    }
}
