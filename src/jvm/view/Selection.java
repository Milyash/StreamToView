package view;

import db.DBConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.*;
import table.tableupdate.rowupdate.CellUpd;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by milya on 27.11.15.
 */
public class Selection {
    private static Logger LOG = LoggerFactory.getLogger(Selection.class);
    private static final String VIEW_TYPE = "select";
    protected String tableName;
    protected boolean isPersistent;
    private ArrayList<ViewField> fields;


    public Selection() {
        super();
        fields = new ArrayList<>();
    }

    public Selection(String tableName, boolean isPersistent) {
        this.tableName = tableName;
        this.isPersistent = isPersistent;
        this.fields = new ArrayList<>();
    }

    public Selection(String tableName) {
        this.tableName = tableName;
        this.isPersistent = true;
        this.fields = new ArrayList<>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public void setIsPersistent(boolean isPersistent) {
        this.isPersistent = isPersistent;
    }

    public ArrayList<ViewField> getFields() {
        return fields;
    }

    public void setFields(ArrayList<ViewField> fields) {
        this.fields = fields;
    }

    public void addField(ViewField field) {
        fields.add(field);
    }

    public String createTableName() {
        StringBuilder sb = new StringBuilder("vt_" + VIEW_TYPE + "_" + tableName);
        for (ViewField field : fields) {
            sb.append("-" + field.getFamilyName() + "_" + field.getColumnName());
        }
        return sb.toString();
    }

    public void processUpdate(TableRowUpd row, String viewName) {
        if (row.getTableName().equals(viewName)) return;

        // LOG.error(name);
        try {
            DBConnector conn = new DBConnector();
            String tableName = row.getTableName();

            if (!row.areUpdatedFieldsInList(fields)) return;// if updated are not used in view

            String pk = row.getPk();
            if (row instanceof TableRowPutUpd) {
                if (!conn.isPkInTable(viewName, pk)) {
                    ArrayList<CellUpd> additionalUpds = conn.getFieldsByPk(viewName, pk, row.getUnupdatedViewFields(fields));
                    row.addCellUpdates(additionalUpds);
                }
                conn.putFieldsByPk(viewName, pk, row.getUpdatedViewCellsUpdates(fields));
            } else if (row instanceof TableRowDeleteUpd) {
                conn.deleteFieldsByPk(viewName, pk, row.getUpdatedFields());
            }


            conn.closeConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public boolean isEmpty() {
        return fields == null || fields.isEmpty();
    }

    @Override
    public String toString() {
        return "Selection{" +
                "fields=" + fields +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
