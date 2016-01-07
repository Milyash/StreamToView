package walentry;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * Created by milya on 11.12.15.
 */
public class Cell implements Serializable {
    private static final long serialVersionUID = 6521635098267757690L;
    private String pk;
    private String columnFamily;
    private String column;
    private boolean isDeletion;
    private byte[] value;

    public Cell(String pk, String columnFamily, String column, boolean isDeletion, byte[] value) {
        this.pk = pk;
        this.columnFamily = columnFamily;
        this.column = column;
        this.isDeletion = isDeletion;
        this.value = value;
    }

    public Cell(org.apache.hadoop.hbase.Cell cell) {
        this.pk = getPkFromCell(cell);
        this.columnFamily = Bytes.toString(cell.getFamily());
        this.column = Bytes.toString(cell.getQualifier());
        this.isDeletion = CellUtil.isDelete(cell);
        this.value = cell.getValue();
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public boolean isDeletion() {
        return isDeletion;
    }

    public void setIsDeletion(boolean isDeletion) {
        this.isDeletion = isDeletion;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }


    public String getPkFromCell(org.apache.hadoop.hbase.Cell cell) {
        if (cell == null) return null;
        String key = CellUtil.getCellKeyAsString(cell);
        String colInfo = "/" + cell.getFamily().toString() + ":" + cell.getQualifier() + "/";
        String pk = null;
        if (key.contains(colInfo))
            pk = key.substring(0, key.indexOf(colInfo));
        return pk;
    }

    @Override
    public String toString() {
        return "Cell{" +
                "pk='" + pk + '\'' +
                ", columnFamily='" + columnFamily + '\'' +
                ", column='" + column + '\'' +
                ", isDeletion=" + isDeletion +
                ", value=" + Bytes.toString(value) +
                '}';
    }
}
