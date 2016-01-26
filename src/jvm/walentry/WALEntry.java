package walentry;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import spout.AppConfig;
import table.tableupdate.CellUpd;
import table.tableupdate.TableRowDeleteUpd;
import table.tableupdate.TableRowPutUpd;
import table.tableupdate.TableRowUpd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by milya on 11.12.15.
 */
public class WALEntry implements Serializable {
    private static final long serialVersionUID = 6521735098267757690L;
    private String tableName;
    private List<Cell> cells;

    public WALEntry(WAL.Entry entry) {
        this.tableName = Bytes.toString(entry.getKey().getTablename().getName());
        this.cells = new ArrayList<Cell>();
        for (org.apache.hadoop.hbase.Cell cell : entry.getEdit().getCells()) {
            this.cells.add(new Cell(cell));
        }
    }

    public WALEntry(String tableName, List<Cell> cells) {
        this.tableName = tableName;
        this.cells = cells;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Cell> getCells() {
        return cells;
    }

    public void setCells(List<Cell> cells) {
        this.cells = cells;
    }

    @Override
    public String toString() {
        return "WALEntry{" +
                "tableName='" + tableName + '\'' +
                ", cells=" + cells +
                '}';
    }

    public ArrayList<TableRowUpd> getTableRowList() {
        HashMap<String, ArrayList<CellUpd>> cellsByPk = new HashMap<>();
        for (Cell cell : cells) {
            if (cell.getColumnFamily().contains(AppConfig.SERVICE_COLUMN_FAMILY_NAME) || cell.getColumn().contains(AppConfig.SERVICE_COLUMN_NAME))
                break;
            String pk = cell.getPk();
            if (!cellsByPk.containsKey(pk))
                cellsByPk.put(pk, new ArrayList<CellUpd>());
            cellsByPk.get(pk).add(new CellUpd(cell, tableName));
        }

        ArrayList<TableRowUpd> rows = new ArrayList<>();
        for (Map.Entry<String, ArrayList<CellUpd>> cellByPk : cellsByPk.entrySet()) {
            String pk = cellByPk.getKey();
            ArrayList cellsUpdList = cellByPk.getValue();
            boolean isDeletion = true;
            for (CellUpd cell : cellByPk.getValue())
                if (!cell.isDeletion()) {
                    isDeletion = false;
                    break;
                }
            TableRowUpd dbRow = null;
            if (isDeletion)
                dbRow = new TableRowDeleteUpd(tableName, pk, cellsUpdList);
            else
                dbRow = new TableRowPutUpd(tableName, pk, cellsUpdList);
            rows.add(dbRow);
        }

        return rows;
    }
}
