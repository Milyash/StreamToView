package view;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.CellUpd;
import table.tableupdate.TableRowDeleteUpd;
import table.tableupdate.TableRowPutUpd;
import table.tableupdate.TableRowUpd;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by milya on 27.11.15.
 */
public class Selection extends ViewPart {
    private static Logger LOG = LoggerFactory.getLogger(Selection.class);
    private static final String VIEW_TYPE = "select";
    protected boolean isPersistent;
    private ArrayList<ViewField> fields;

    public Selection() {
        super();
        this.isPersistent = true;
        this.fields = new ArrayList<>();
    }

    public Selection(ArrayList<ViewField> fields) {
        this.isPersistent = true;
        this.fields = fields;
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

    @Override
    public String getId() {
        StringBuilder sb = new StringBuilder(VIEW_TYPE + "-");
        for (ViewField field : fields) {
            sb.append("_" + field.getTableName() + "-" + field.getFamilyName() + "-" + field.getColumnName());
        }
        return sb.toString();
    }

    @Override
    public boolean processUpdate(TableRowUpd row, String viewName) {
        if (row.getTableName().equals(viewName)) return true;

        try {
            if (!row.areViewFieldsUpdated(fields)) return true;// if updated are not used in view

            String pk = row.getPk();
            if (row instanceof TableRowPutUpd) {
                if (!conn.isPkInTable(viewName, pk)) {
                    ArrayList<CellUpd> additionalUpds = conn.getTableFieldsByPk(row.getTableName(), pk, row.getUnUpdatedViewFields(fields));
                    row.addCellUpdates(additionalUpds);
                }

                conn.putViewFieldsByPk(viewName, pk, row.getUpdatedViewCells(fields));
            } else if (row instanceof TableRowDeleteUpd) {
                conn.deleteViewFieldsByPk(viewName, pk, row.getUpdatedFields());
            }
            return true;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    public boolean isEmpty() {
        return fields == null || fields.isEmpty();
    }

    @Override
    public String toString() {
        return "Selection{" +
                "fields=" + fields +
                '}';
    }
}
