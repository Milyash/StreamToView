package table;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import table.value.Value;
import view.ViewField;

import java.io.Serializable;

/**
 * Created by milya on 16.11.15.
 */
public class Field implements Serializable {

    protected String columnName;
    protected String familyName;
    protected Value.TYPE dataType;


    public Field() {
    }

    public Field(String columnName, String familyName, Value.TYPE dataType) {
        this.columnName = columnName;
        this.familyName = familyName;
        this.dataType = dataType;
    }

    public Field(String columnName, String familyName) {
        this.columnName = columnName;
        this.familyName = familyName;
    }

    public Field(Cell cell) {
        this.columnName = Bytes.toString(cell.getQualifier());
        this.familyName = Bytes.toString(cell.getFamily());
    }

    public Field(walentry.Cell cell) {
        this.columnName = cell.getColumn();
        this.familyName = cell.getColumnFamily();
    }

    public Field(Field viewField) {
        this.columnName = viewField.getColumnName();
        this.familyName = viewField.getFamilyName();
        this.dataType = viewField.getDataType();
    }

    public ViewField toViewField(String tableName) {
        return new ViewField(tableName, this);
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String name) {
        this.columnName = name;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public Value.TYPE getDataType() {
        return dataType;
    }

    public void setDataType(Value.TYPE dataType) {
        this.dataType = dataType;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Field)) return false;
        Field tableField = (Field) obj;
        if (columnName.equals(tableField.getColumnName()))
            if (familyName.equals(tableField.getFamilyName())) // TODO should type comparison be here???
                return true;
        return false;
    }

    public String toColumnName() {
        return familyName + "_" + columnName;
    }

    @Override
    public String toString() {
        return "Field{" +
                "columnName='" + columnName + '\'' +
                ", familyName='" + familyName + '\'' +
                ", dataType=" + dataType +
                '}';
    }

    public String contentToString() {
        return "columnName='" + columnName +
                ", familyName='" + familyName +
                ", dataType=" + dataType;
    }
}
