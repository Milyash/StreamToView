package view;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.Field;
import table.value.Value;

/**
 * Created by milya on 20.12.15.
 */
public class ViewField extends Field {
    private static Logger LOG = LoggerFactory.getLogger(ViewField.class);
    private String tableName;

    public ViewField() {
    }

    public ViewField(String tableName, String columnName, String familyName, Value.TYPE dataType) {
        super(columnName, familyName, dataType);
        this.tableName = tableName;
    }

    public ViewField(String tableName, Field field) {
        super(field);
        this.tableName = tableName;
    }

    public ViewField(String tableName, walentry.Cell cell) {
        super(cell);
        this.tableName = tableName;
    }

    public ViewField(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public String toColumnName() {
        return tableName + "_" + super.toColumnName();
    }

    @Override
    public String toString() {
        return "ViewField{" +
                "tableName='" + tableName + "; " + super.contentToString() + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ViewField)) return false;
        ViewField viewField = (ViewField) obj;

        if (tableName.equals(viewField.getTableName()))
            if (familyName.equals(viewField.getFamilyName()))
                if (columnName.equals(viewField.getColumnName()))// TODO should type comparison be here???
                    return true;
        return false;
    }
}
