package bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import db.DBConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.Field;
import table.Table;
import table.tableupdate.CellUpd;
import table.tableupdate.TableRowDeleteUpd;
import table.tableupdate.TableRowUpd;
import utils.TableXMLDefinitionFacotry;
import utils.TableXMLViewFactory;
import view.Const;
import view.View;
import view.ViewField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by milya on 16.11.15.
 */
public class ViewCollectionBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ViewCollectionBolt.class);
    private static final String LOG_STRING = "------ t: ";

    public static final String VIEW_SELECTION_DISTRIBUTION_STREAM = "View selection distribution stream";
    public static final String VIEW_WHERE_DISTRIBUTION_STREAM = "View where distribution stream";
    public static final String VIEW_AGGREGATES_DISTRIBUTION_STREAM = "View aggregates by distribution stream";
    public static final String VIEW_HAVING_DISTRIBUTION_STREAM = "View having by distribution stream";

    public static final String UPDATE_DISTRIBUTION_STREAM = "ViewCollectionBolt: Update distribution stream";

    public OutputCollector _outputCollector;
    public Map<String, Table> tables;
    private boolean updateViews;
    private final static DBConnector conn = new DBConnector();

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120);
        return conf;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
        tables = new HashMap<>();
        updateViews = false;
    }

    @Override
    public void execute(Tuple tuple) {

        if (isTickTuple(tuple)) {
            updateTableViews();
            updateViews = true;
        } else {

            TableRowUpd tableUpdate = (TableRowUpd) tuple.getValueByField("update");

            LOG.error(LOG_STRING + " get update: " + tableUpdate);

            if (tableUpdate == null) {
                _outputCollector.ack(tuple);
                return;
            }
            String tableName = tableUpdate.getTableName();

            if (!tables.containsKey(tableName)) {
                startWatchingTable(tableName);
                updateViews = true;
            }
            if (updateViews) {
                for (View view : getViews()) {
                    String viewName = view.getName();
                    _outputCollector.emit(VIEW_SELECTION_DISTRIBUTION_STREAM, tuple, new Values(viewName, view.getSelection()));
                    _outputCollector.emit(VIEW_WHERE_DISTRIBUTION_STREAM, tuple, new Values(viewName, view.getWhere()));
                    _outputCollector.emit(VIEW_AGGREGATES_DISTRIBUTION_STREAM, tuple, new Values(viewName, view.getAggregates()));
                    _outputCollector.emit(VIEW_HAVING_DISTRIBUTION_STREAM, tuple, new Values(viewName, view.getHaving()));
                }
                updateViews = false;
            }


            if (tableUpdate instanceof TableRowDeleteUpd && tableUpdate.getCellUpdates().isEmpty())
                for (ViewField tableField : tables.get(tableName).getViewFields())
                    tableUpdate.addCellUpdate(new CellUpd(true, tableField, null));

            else {
                try {
                    ArrayList<CellUpd> updateCells = conn.getTableFieldsByPk(tableName,
                            tableUpdate.getPk(),
                            tableUpdate.getUnUpdatedViewFields(tables.get(tableName).getViewFields()));
                    tableUpdate.addCellUpdates(updateCells);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            LOG.error(LOG_STRING + " tables: " + tables);
            for (View view : tables.get(tableName).getViews()) {
                LOG.error(LOG_STRING + " tables: " + tables.toString());
                LOG.error(LOG_STRING + " emit: viewNAme: " + view.getName() + "; update: " + tableUpdate);
                _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(view.getName(), tableUpdate));
            }


        }

        _outputCollector.ack(tuple);
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void updateTableViews() {
        for (String tableName : tables.keySet()) {
            tables.get(tableName).setViews(TableXMLViewFactory.getViewsByTableName(tableName));
        }
    }

    private void startWatchingTable(String tableName) {
        Table table = new Table(tableName);
        List<Field> fields = TableXMLDefinitionFacotry.getFieldsDefinitionByTableName(tableName);
        if (fields.size() == 0) return;

        table.setFields(fields);

        ArrayList<View> views = (ArrayList<View>) TableXMLViewFactory.getViewsByTableName(tableName);
        if (views.size() == 0) return;

        table.setViews(views);
        LOG.error(LOG_STRING + " startWatchingTable: table: " + table.toString() + "; views: " + views.toString());

        tables.put(tableName, table);

        createHistoryTables(table);
        createViewTables(views);
    }

    private List<View> getViews() {
        List<View> views = new ArrayList<>();
        for (Map.Entry<String, Table> tableEntry : tables.entrySet()) {
            Table table = tableEntry.getValue();
            views.addAll(table.getViews());
        }
        return views;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(VIEW_SELECTION_DISTRIBUTION_STREAM, new Fields("viewName", "selects"));
        outputFieldsDeclarer.declareStream(VIEW_WHERE_DISTRIBUTION_STREAM, new Fields("viewName", "wheres"));
        outputFieldsDeclarer.declareStream(UPDATE_DISTRIBUTION_STREAM, new Fields("viewName", "update"));
        outputFieldsDeclarer.declareStream(VIEW_AGGREGATES_DISTRIBUTION_STREAM, new Fields("viewName", "aggregates"));
        outputFieldsDeclarer.declareStream(VIEW_HAVING_DISTRIBUTION_STREAM, new Fields("viewName", "havings"));
    }

    private void createViewTables(ArrayList<View> views) {
        for (View view : views) {
            conn.createTable(view.getName());
        }
    }

    private void createHistoryTables(Table table) {
        try {
            for (View view : table.getViews()) {
                if (view.getMaxes() != null
                        || view.getMins() != null
                        || view.getSums() != null
                        || view.getCounts() != null) {
                    String historyTableName = Const.HISTORY_TABLE_NAME_PREFIX + table.getName();
                    if (!conn.tableExists(historyTableName))
                        conn.createTable(historyTableName, table.getFieldsColumnFamilies());

                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
