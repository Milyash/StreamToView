package bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.CellUpd;
import table.tableupdate.TableRowDeleteUpd;
import table.tableupdate.TableRowUpd;
import view.Selection;
import view.ViewField;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by milya on 19.11.15.
 */
public class SelectionBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SelectionBolt.class);
    private static final String LOG_STRING = "------ SelectionBolt: ";

    public OutputCollector _outputCollector;
    private Map<String, Selection> selections;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
        selections = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String viewName = (String) tuple.getValueByField("viewName");

        switch (tuple.getSourceStreamId()) {
            case (HavingBolt.UPDATE_DISTRIBUTION_STREAM):
                TableRowUpd update = (TableRowUpd) tuple.getValueByField("update");
                if (update == null) break;
                String logString = " get: viewName: " + viewName + "; update: " + update;
                LOG.error(LOG_STRING + logString);

                if (!viewName.equals("")) {
                    if (selections.containsKey(viewName))
                        selections.get(viewName).processUpdate(update, viewName);
                } else
                    for (Map.Entry<String, Selection> selectionByViewName : selections.entrySet()) {
                        Selection selection = selectionByViewName.getValue();
                        viewName = selectionByViewName.getKey();
                        if (update instanceof TableRowDeleteUpd && update.getCellUpdates().isEmpty()) {
                            for (ViewField selectionField : selection.getFields())
                                update.addCellUpdate(new CellUpd(true, selectionField, null));
                        }
                        selection.processUpdate(update, viewName);
                    }

                _outputCollector.emit(tuple, new Values(update));
                break;

            case (ViewCollectionBolt.VIEW_SELECTION_DISTRIBUTION_STREAM):
                Selection selection = (Selection) tuple.getValueByField("selects");
                selections.put(viewName, selection);
                break;

        }
        _outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tableUpdate"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120);
        return conf;
    }
}
