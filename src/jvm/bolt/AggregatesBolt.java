package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.TableRowUpd;
import view.ViewField;
import view.grouped.Aggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by milya on 07.01.16.
 */
public class AggregatesBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AggregatesBolt.class);
    private static final String LOG_STRING = "------ AggregatesBolt: ";
    private HistoryTableController historyController;

    public static final String UPDATE_DISTRIBUTION_STREAM = "AggregatesBolt: Update distribution stream";

    public OutputCollector _outputCollector;
    private HashMap<String, ArrayList<Aggregate>> aggregates;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
        aggregates = new HashMap<>();
        historyController = new HistoryTableController();
    }

    @Override
    public void execute(Tuple tuple) {
        String viewName = (String) tuple.getValueByField("viewName");
        switch (tuple.getSourceStreamId()) {

            case (WhereBolt.UPDATE_DISTRIBUTION_STREAM):

                TableRowUpd update = (TableRowUpd) tuple.getValueByField("update");

                String logString = " get: viewName: " + viewName + "; update: " + update;

                LOG.error(LOG_STRING + logString);

                if (update == null) {
                    _outputCollector.ack(tuple);
                    return;
                }

                if (aggregates.size() == 0) {
                    LOG.error(LOG_STRING + " no groupped aggrs => pass of" + logString);
                    _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(viewName, update));
                } else {

                    ArrayList<Aggregate> viewAggregates = aggregates.get(viewName);

                    if (viewAggregates != null) {
                        HashMap<ViewField, Object> historyEntry = historyController.getHistoryRow(viewAggregates, update.getPk(), update.getTableName());
                        historyController.updateHistoryTable(update);
                        for (Aggregate viewAggregate : viewAggregates) {

                            ArrayList<TableRowUpd> updatesToPass = viewAggregate.processAggregateUpdate(update, viewName, historyEntry);

                            LOG.error(LOG_STRING + viewAggregate.VIEW_TYPE + ":  " + viewAggregate
                                    + "\n for view: " + viewName + " is Met "
                                    + "\n => pass of " + logString
                                    + "\n pass updated:" + updatesToPass);

                            if (updatesToPass == null) continue;
                            for (TableRowUpd updateToPass : updatesToPass)
                                _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(viewName, updateToPass));
                        }
                    }
                }
                break;

            case (ViewCollectionBolt.VIEW_AGGREGATES_DISTRIBUTION_STREAM):
//                ArrayList<Max> maxs = (ArrayList<Max>) tuple.getValueByField("maxs");
//                ArrayList<Min> mins = (ArrayList<Min>) tuple.getValueByField("mins");
//                ArrayList<Sum> sums = (ArrayList<Sum>) tuple.getValueByField("sums");
//                ArrayList<Count> counts = (ArrayList<Count>) tuple.getValueByField("counts");
//
//                aggregates.put(viewName, new ArrayList<Aggregate>());
//                if (maxs != null) aggregates.get(viewName).addAll(maxs);
//                if (mins != null) aggregates.get(viewName).addAll(mins);
//                if (sums != null) aggregates.get(viewName).addAll(sums);
//                if (counts != null) aggregates.get(viewName).addAll(counts);

                aggregates.put(viewName, new ArrayList<Aggregate>());
                ArrayList<Aggregate> aggs = (ArrayList<Aggregate>) tuple.getValueByField("aggregates");
                if (aggs != null)
                    aggregates.get(viewName).addAll(aggs);
                for (Aggregate aggregate : aggregates.get(viewName))
                    aggregate.prepareTable();

                break;
        }
        _outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(UPDATE_DISTRIBUTION_STREAM, new Fields("viewName", "update"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
