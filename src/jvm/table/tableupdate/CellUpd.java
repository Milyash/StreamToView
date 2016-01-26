package table.tableupdate;

import org.apache.hadoop.hbase.util.Bytes;
import view.ViewField;
import walentry.Cell;

import java.io.Serializable;

/**
 * Created by milya on 17.12.15.
 */
public class CellUpd implements Serializable {
    private boolean isDeletion;
    private ViewField field;
    private byte[] value;

    public CellUpd(boolean isDeletion, ViewField field, byte[] value) {
        this.isDeletion = isDeletion;
        this.field = field;
        this.value = value;
    }

    public CellUpd(boolean isDeletion, ViewField field, Object value) {

        this(isDeletion, field, getBytes(value));
    }

    public CellUpd(Cell cell, String tableName) {
        this.isDeletion = cell.isDeletion();
        this.field = new ViewField(tableName, cell);
        this.value = cell.getValue();
    }

    public boolean isDeletion() {
        return isDeletion;
    }

    public void setIsDeletion(boolean isDeletion) {
        this.isDeletion = isDeletion;
    }

    public ViewField getField() {
        return field;
    }

    public void setField(ViewField field) {
        this.field = field;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    private static byte[] getBytes(Object o) {
        if (o != null)
            return Bytes.toBytes(o.toString());
        return null;
    }

    @Override
    public String toString() {
        return "CellUpd{" +
                "isDeletion=" + isDeletion +
                ", field=" + field +
                ", value=" + Bytes.toString(value) +
                '}';
    }
}
