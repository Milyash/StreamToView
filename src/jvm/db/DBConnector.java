package db;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.CellUpd;
import table.tableupdate.TableRowUpd;
import table.value.ValueFabric;
import view.ViewField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
        createTable(tableName, null);
    }

    public void createTable(String tableName, ArrayList<String> columnFamilies) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        if (columnFamilies != null)
            for (String columnFamily : columnFamilies)
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
        else
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

        LOG.error(LOG_STRING + " isPkInTable: " + tableName + " entry " + result.toString() + " isEmpty: " + result.isEmpty());
        return !result.isEmpty();
    }

    public HashMap<ViewField, Object> getFieldsByPk(String tableName, String pk, ArrayList<ViewField> fields) throws IOException {

        if (!tableExists(tableName)) return null;
        HTable table = new HTable(config, tableName);

        Get get = new Get(getBytes(pk));
        for (ViewField field : fields)
            if (field != null)
                get.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));

        Result result = table.get(get);
        HashMap<ViewField, Object> cellUpds = new HashMap<>();
        for (ViewField field : fields) {
            if (field == null) continue;
            String column = field.getColumnName();
            String columnFamily = field.getFamilyName();

            byte[] byteValue = null;
            if (result.containsColumn(getBytes(columnFamily), getBytes(column))) {
                byteValue = result.getValue(getBytes(columnFamily), getBytes(column));

                cellUpds.put(field, ValueFabric.getValue(byteValue, field.getDataType()));
            }
        }

        return cellUpds;
    }

    public Object getFieldByPk(String tableName, String pk, ViewField field) throws IOException {

        return getFieldsByPk(tableName, pk, Lists.newArrayList(field)).get(field);
    }

    public ArrayList<CellUpd> getTableFieldsByPk(String tableName, String pk, ArrayList<ViewField> fields) throws IOException {

        if (!tableExists(tableName)) return null;
        HTable table = new HTable(config, tableName);

        Get get = new Get(getBytes(pk));
        for (ViewField field : fields)
            get.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));

        Result result = table.get(get);
        ArrayList<CellUpd> cellUpds = new ArrayList<>();
        for (ViewField field : fields) {
            String column = field.getColumnName();
            String columnFamily = field.getFamilyName();
            cellUpds.add(new CellUpd(false, field, result.getValue(getBytes(columnFamily), getBytes(column))));
        }

        LOG.error(LOG_STRING + " getFieldsByPk " + tableName + "unupdated data: " + cellUpds.toString());
        return cellUpds;
    }

    public void putViewFieldsByPk(String tableName, String pk, ArrayList<CellUpd> cellUpds) throws IOException {
        HTable table = getTable(tableName);

        LOG.error(LOG_STRING + "Table: " + tableName + " -- " + table.toString());

        Put put = new Put(getBytes(pk));
        for (CellUpd cellUpd : cellUpds)
            put.addColumn(getBytes(DEFAULT_COLUMN_FAMILY), getBytes(getColumnName(cellUpd.getField())), cellUpd.getValue());

        table.put(put);
    }

    public void putFieldsByPk(String tableName, String pk, HashMap<ViewField, Object> fieldUpdates) throws IOException {
        HTable table = getTable(tableName);

        LOG.error(LOG_STRING + "Table: " + tableName + " -- " + table.toString());

        Put put = new Put(getBytes(pk));
        Delete delete = new Delete(getBytes(pk));
        for (Map.Entry<ViewField, Object> fieldUpdate : fieldUpdates.entrySet()) {
            Object value = fieldUpdate.getValue();
            ViewField field = fieldUpdate.getKey();
            if (value == null) delete.addColumns(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));
            else put.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()), getBytes(value));
        }

        if (!delete.isEmpty())
            table.delete(delete);
        if (!put.isEmpty())
            table.put(put);
    }

    public void putByteFieldsByPk(String tableName, String pk, HashMap<ViewField, byte[]> fieldUpdates) throws IOException {
        HTable table = getTable(tableName);

        LOG.error(LOG_STRING + "Table: " + tableName + " -- " + table.toString());

        Put put = new Put(getBytes(pk));
        Delete delete = new Delete(getBytes(pk));
        for (Map.Entry<ViewField, byte[]> fieldUpdate : fieldUpdates.entrySet()) {
            byte[] value = fieldUpdate.getValue();
            ViewField field = fieldUpdate.getKey();
            if (value == null) delete.addColumns(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));
            else put.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()), value);
        }

        if (!delete.isEmpty())
            table.delete(delete);
        if (!put.isEmpty())
            table.put(put);
    }

    public void putFieldByPk(String tableName, String pk, ViewField field, Object value) throws IOException {
        HTable table = getTable(tableName);

        LOG.error(LOG_STRING + "Table: " + tableName + " -- " + table.toString());

        if (value != null) {
            Put put = new Put(getBytes(pk));
            put.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()), getBytes(value));
            table.put(put);
        } else {
            Delete delete = new Delete(getBytes(pk));
            delete.addColumns(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));
            table.delete(delete);
        }
    }

    public void putRowsByPk(String tableName, TableRowUpd update) throws IOException {
        HTable table = getTable(tableName);
        String pk = update.getPk();

        Put put = new Put(getBytes(pk));
        for (CellUpd cellUpd : update.getCellUpdates())
            put.addColumn(getBytes(DEFAULT_COLUMN_FAMILY), getBytes(getColumnName(cellUpd.getField())), cellUpd.getValue());

        table.put(put);
    }

    public void deleteViewFieldsByPk(String tableName, String pk, ArrayList<ViewField> fields) throws IOException {
        HTable table = getTable(tableName);

        Delete delete = new Delete(getBytes(pk));
        for (ViewField tableField : fields)
            delete.addColumns(getBytes(DEFAULT_COLUMN_FAMILY), getBytes(getColumnName(tableField)));
        LOG.error(LOG_STRING + " delete : " + tableName + " ---- " + delete.toString());
        table.delete(delete);
    }

    public void deleteFieldsByPk(String tableName, String pk, ArrayList<ViewField> fields) throws IOException {
        HTable table = getTable(tableName);

        Delete delete = new Delete(getBytes(pk));
        for (ViewField field : fields) {
            if (field == null) continue;
            delete.addColumns(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));
        }
        LOG.error(LOG_STRING + " delete : " + tableName + " ---- " + delete.toString());
        table.delete(delete);
    }

    public void deleteRowByPk(String tableName, String pk) throws IOException {
        HTable table = getTable(tableName);

        Delete delete = new Delete(getBytes(pk));
        LOG.error(LOG_STRING + " deleteRowByPk : " + tableName + " pk: " + pk);

        table.delete(delete);
    }

    public <T> HashMap<String, T> scanTableFields(String tableName, ViewField field) throws IOException {

        HTable table = new HTable(config, tableName);
        Scan scan = new Scan();
        scan.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));

        ResultScanner scanner = table.getScanner(scan);

        HashMap<String, T> scannedList = new HashMap<>();
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            byte[] byteValue = result.getValue(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));
            scannedList.put(getPk(result), (T) ValueFabric.getValue(byteValue, field.getDataType()));
        }

        scanner.close();
        return scannedList;
    }

    public <T> HashMap<String, HashMap<ViewField, Object>> scanTableFields(String tableName, ArrayList<ViewField> fields) throws IOException {

        HTable table = new HTable(config, tableName);
        Scan scan = new Scan();
        for (ViewField field : fields)
            if (field != null)
                scan.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));

        ResultScanner scanner = table.getScanner(scan);

        HashMap<String, HashMap<ViewField, Object>> scannedList = new HashMap<>();
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            HashMap<ViewField, Object> resultRow = new HashMap<>();
            for (ViewField field : fields) {
                if (field == null) continue;
                byte[] byteValue = null;
                if (result.containsColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()))) {
                    byteValue = result.getValue(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));
                }

                resultRow.put(field, (T) ValueFabric.getValue(byteValue, field.getDataType()));
            }
            scannedList.put(getPk(result), resultRow);
        }

        scanner.close();
        return scannedList;
    }

    public <T> ArrayList<T> scanTableField(String tableName, ViewField field) throws IOException {

        HTable table = new HTable(config, tableName);
        Scan scan = new Scan();
        scan.addColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));

        ResultScanner scanner = table.getScanner(scan);

        ArrayList<T> list = new ArrayList<>();
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            byte[] byteValue = null;
            if (result.containsColumn(getBytes(field.getFamilyName()), getBytes(field.getColumnName()))) {
                byteValue = result.getValue(getBytes(field.getFamilyName()), getBytes(field.getColumnName()));
            }

            list.add((T) ValueFabric.getValue(byteValue, field.getDataType()));
        }

        scanner.close();
        return list;
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

    protected static String getPk(Result r) {
        return Bytes.toString(r.getRow());
    }
}
