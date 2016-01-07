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
import table.tableupdate.*;
import table.Table;
import utils.TableXMLDefinitionFacotry;
import utils.TableXMLViewFactory;
import view.Selection;
import view.View;

import java.util.*;

/**
 * Created by milya on 16.11.15.
 */
public class ViewCollectionBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(ViewCollectionBolt.class);
    public static final String LOG_STRING = "------ ViewCollectionBolt: ";
    public static final String BOLT_NAME = "View Collection Bolt";
    public static final String VIEW_SELECTION_DISTRIBUTION_STREAM = "View selection distribution stream";
    public static final String VIEW_WHERE_DISTRIBUTION_STREAM = "View where distribution stream";
    public static final String VIEW_DISTRIBUTION_STREAM = "View distribution stream";
    public static final String UPDATE_DISTRIBUTION_STREAM = "ViewCollectionBolt: Update distribution stream";

    public OutputCollector _outputCollector;
    public Map<String, Table> tables;
    private boolean updateViews;

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

            TableUpd tableUpdate = (TableUpd) tuple.getValueByField("update");
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
                }
                updateViews = false;
            }

            for (TableRowUpd rowUpdate : tableUpdate.getTableRowUpdates()) {
                _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(rowUpdate));
//                LOG.error(LOG_STRING + " upd: " + rowUpdate.toString());
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
            for (View view : TableXMLViewFactory.getViewsByTableName(tableName))
                tables.get(tableName).addView(view);
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

        createViewTables(views);

    }

    private List<View> getViews() {
        List<View> views = new ArrayList<>();
        for (Map.Entry<String, Table> tableEntry : tables.entrySet()) {
            Table table = tableEntry.getValue();
//            LOG.error(LOG_STRING + " getViews: table: " + table.toString());
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
        outputFieldsDeclarer.declareStream(UPDATE_DISTRIBUTION_STREAM, new Fields("update"));
    }

    private void createViewTables(ArrayList<View> views) {
        DBConnector conn = new DBConnector();
        for (View view : views) {
            conn.createTable(view.getName());
        }
    }
}
