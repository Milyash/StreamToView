package table.tableupdate.rowupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import view.ViewField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by milya on 17.12.15.
 */
public class DBRow implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(DBRow.class);
    private String pk;
    private ArrayList<CellUpd> cellUpdates;

    public DBRow() {
    }

    public DBRow(String pk) {
        this.pk = pk;
        this.cellUpdates = new ArrayList<>();
    }

    public DBRow(String pk, ArrayList<CellUpd> cellUpdates) {
        this.pk = pk;
        this.cellUpdates = cellUpdates;
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
        if (cellUpd == null) return;
        if (cellUpdates == null) cellUpdates = new ArrayList<>();
        cellUpdates.add(cellUpd);
    }

    public void addCellUpdates(List<CellUpd> cellUpds) {
        if (cellUpds == null || cellUpds.size() == 0) return;
        if (cellUpdates == null) cellUpdates = new ArrayList<>();
        cellUpdates.addAll(cellUpds);
    }

    @Override
    public String toString() {
        return "DBRow{" +
                "pk='" + pk + '\'' +
                ", cellUpdates=" + cellUpdates +
                '}';
    }

    public ArrayList<ViewField> getUpdatedFields() {
        ArrayList<ViewField> fields = new ArrayList<>();
        for (CellUpd cellUpd : cellUpdates)
            fields.add(cellUpd.getField());
        return fields;
    }

    public ArrayList<ViewField> getUnUpdatedFieldsFromList(ArrayList<ViewField> viewFields) {
        ArrayList<ViewField> fieldsCopy = (ArrayList<ViewField>) viewFields.clone(); // are parameters sent by reference???
//        LOG.error(" =================== unupdated fields: " + fieldsCopy.toString() + " - " + row.getUpdatedFields().toString() + " = " + fieldsCopy.toString());
        fieldsCopy.removeAll(getUpdatedFields()); // fields \\ updated fields
//        LOG.error(" =================== unupdated fields: " + fieldsCopy.toString());
        return fieldsCopy;
    }

    public ArrayList<ViewField> getUpdatedFieldsFromList(ArrayList<ViewField> viewFields) {
        ArrayList<ViewField> updatedFields = getUpdatedFields();
        ArrayList<ViewField> intersection = getIntersection(updatedFields, viewFields); // updated fields and fields - intersection
//        LOG.error(" =================== DBROW getUpdatedFieldsFromList fields: " + updatedFields + " and " + viewFields + " == " + intersection);
        return intersection;
    }

    public boolean areFieldsFromListUnupdated(ArrayList<ViewField> viewFields) {
        return getUpdatedFieldsFromList(viewFields).size() == 0;// if updated are not used in view
    }

    public boolean areFieldsFromListUpdated(ViewField viewField) {
        ArrayList<ViewField> viewFields = new ArrayList<ViewField>(Arrays.asList(viewField));
        return getUpdatedFieldsFromList(viewFields).size() > 0;// if updated are used in view
    }

    public byte[] getUpdatedValueByField(ViewField field) {
        for (CellUpd cellUpd : cellUpdates) {
            if (cellUpd.getField().equals(field))
                return cellUpd.getValue();
        }
        return null;
    }


    private <T> ArrayList<T> getIntersection(ArrayList<T> fromList, ArrayList<T> list) {

        ArrayList<T> intersection = new ArrayList<>();
        for (T fromListItem : fromList)
            for (T listItem : list)
                if (fromListItem.equals(listItem))
                    intersection.add(fromListItem);

        return intersection;
    }
}
