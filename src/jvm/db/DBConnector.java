package db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.rowupdate.CellUpd;
import table.tableupdate.TableRowUpd;
import view.ViewField;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by milya on 09.12.15.
 */
public class DBConnector {
    private Connection conn;
    private Configuration config;
    private Admin admin;
    private static Logger LOG = LoggerFactory.getLogger(DBConnector.class);
    private static final String DEFAULT_COLUMN_FAMILY = "default";

    private static final String LOG_STRING = "************ DBConnector: ";

    public DBConnector() {
        config = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(config);
            admin = conn.getAdmin();
        } catch (IOException e) {
            LOG.error(LOG_STRING + "Error establishing connection to database!");
            e.printStackTrace();
        }
    }

    public boolean tableExists(String tableName) throws IOException {
        LOG.error(LOG_STRING + " tableExists: " + TableName.valueOf(tableName).getNameAsString() + " is " + admin.tableExists(TableName.valueOf(tableName)));
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public void createTable(String tableName) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor(DEFAULT_COLUMN_FAMILY));
        try {
            if (!tableExists(tableName)) {
                LOG.error(LOG_STRING + " createTable: " + tableName);
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            LOG.error(LOG_STRING + " createTable: Error creating view table");
        }
    }

    private HTable getTable(String tableName) throws IOException {

        if (!tableExists(tableName)) {
            // createTable(tableName);
//            LOG.error(LOG_STRING + "Table " + tableName + " doesn't exist!");
            return null;
        }
        return new HTable(config, tableName);
    }

    public boolean isPkInTable(String tableName, String pk) throws IOException {
        if (!tableExists(tableName)) return false;
        Get get = new Get(getBytes(pk));
        HTable table = new HTable(config, tableName);
        Result result = table.get(get);

        LOG.error(LOG_STRING + " isPkInTable: " + result.toString() + " isEmpty: " + result.isEmpty());
        return !result.isEmpty();
    }

    public ArrayList<CellUpd> getFieldsByPk(String tableName, String pk, ArrayList<ViewField> fields) throws IOException {

        if (!tableExists(tableName)) return null;
        HTable table = new HTable(config, tableName);

        Get get = new Get(getBytes(pk));
        for (ViewField field : fields)
            get.addColumn(getBytes(DEFAULT_COLUMN_FAMILY), getBytes(getColumnName(field)));

        Result result = table.get(get);
        ArrayList<CellUpd> cellUpds = new ArrayList<>();
        for (ViewField field : fields) {
            String column = field.toColumnName();
            String columnFamily = field.getFamilyName();
            cellUpds.add(new CellUpd(false, field, result.getValue(getBytes(columnFamily), getBytes(column))));
        }

        LOG.error(LOG_STRING + " getFieldsByPk unupdated data: " + cellUpds.toString());
        return cellUpds;
    }

    public void putFieldsByPk(String tableName, String pk, ArrayList<CellUpd> cellUpds) throws IOException {
        HTable table = getTable(tableName);

        LOG.error(LOG_STRING + "Table: " + tableName + " -- " + table.toString());

        Put put = new Put(getBytes(pk));
        for (CellUpd cellUpd : cellUpds)
            put.addColumn(getBytes(DEFAULT_COLUMN_FAMILY), getBytes(getColumnName(cellUpd.getField())), cellUpd.getValue());

        table.put(put);
    }

    public void putRowsByPk(String tableName, TableRowUpd update) throws IOException {
        HTable table = getTable(tableName);
        String pk = update.getPk();

        Put put = new Put(getBytes(pk));
        for (CellUpd cellUpd : update.getCellUpdates())
            put.addColumn(getBytes(DEFAULT_COLUMN_FAMILY), getBytes(getColumnName(cellUpd.getField())), cellUpd.getValue());

        table.put(put);
    }

    public void deleteFieldsByPk(String tableName, String pk, ArrayList<ViewField> fields) throws IOException {
        HTable table = getTable(tableName);

        Delete delete = new Delete(getBytes(pk));
        for (ViewField tableField : fields)
            delete.addColumns(getBytes(DEFAULT_COLUMN_FAMILY), getBytes(getColumnName(tableField)));
        LOG.error(LOG_STRING + " delete : " + delete.toString());
        table.delete(delete);
    }

    public void deleteRowByPk(String tableName, String pk) throws IOException {
        HTable table = getTable(tableName);

        Delete delete = new Delete(getBytes(pk));
        LOG.error(LOG_STRING + " deleteRowByPk : " + pk);

        table.delete(delete);
    }

    private String getColumnName(ViewField field) {
        String tableName = field.getTableName();
        String column = field.getColumnName();
        String columnFamily = field.getFamilyName();
        return tableName + "::" + columnFamily + ":" + column;
    }

    public void closeConnection() throws IOException {
        conn.close();
    }

    private byte[] getBytes(Object o) {
        if (o != null)
            return Bytes.toBytes(o.toString());
        return null;
    }
}
