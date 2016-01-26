package table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.value.Value;
import utils.TableXMLDefinitionFacotry;
import view.View;
import view.ViewField;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by milya on 16.11.15.
 */
public class Table {
    private static final Logger LOG = LoggerFactory.getLogger(Table.class);
    private String name;
    private boolean storeHistory;
    private List<Field> fields;
    private List<View> views;

    public Table(String name) {
        this.name = name;
    }

    public Table(String name, List<Field> fields) {
        this.name = name;
        this.fields = fields;
    }

    public Table(String name, List<Field> fields, List<View> views) {
        this.name = name;
        this.fields = fields;
        this.views = views;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isStoreHistory() {
        return storeHistory;
    }

    public void setStoreHistory(boolean storeHistory) {
        this.storeHistory = storeHistory;
    }

    public List<Field> getFields() {
        return fields;
    }

    public ArrayList<ViewField> getViewFields() {
        ArrayList<ViewField> viewFields = new ArrayList<>();
        for (Field field : fields) {
            viewFields.add(new ViewField(name, field));
        }
        return viewFields;
    }

    public ArrayList<String> getFieldsColumnFamilies() {
        ArrayList<String> fieldColumnFamilies = new ArrayList<>();
        for (Field field : fields)
            if (!fieldColumnFamilies.contains(field.familyName))
                fieldColumnFamilies.add(field.familyName);
        return fieldColumnFamilies;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public List<View> getViews() {
        return views;
    }

    public void setViews(List<View> views) {
        this.views = views;
    }

    public void addView(View view) {
        this.views.add(view);
    }

    /* DATA TYPE */
    public Value.TYPE getDataTypeByField(Field byField) {
        Value.TYPE dataType = byField.getDataType();

        if (dataType != null && dataType != Value.TYPE.BYTES) return dataType;

        for (Field field : fields) {
            if (field.equals(byField))
                return field.getDataType();
        }
        return Value.TYPE.BYTES;
    }

    /* VIEW */
    public View getViewByName(String viewName) {
        for (View tableView : views) {
            if (tableView.getName().equals(viewName.trim()))
                return tableView;
        }
        return null;
    }

    /* LOAD FROM XML */
    public static Table loadTable(String name) {
        return TableXMLDefinitionFacotry.getTableDefinitionByTableName(name);
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name +
                ", fields=" + fields +
                ", views=" + views +
                '}';
    }
}
