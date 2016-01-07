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
import table.tableupdate.TableRowUpd;
import view.Selection;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by milya on 19.11.15.
 */
public class SelectionBolt implements IRichBolt {
    public static final String BOLT_NAME = "Select Bolt";
    private static Logger LOG = LoggerFactory.getLogger(SelectionBolt.class);
    private OutputCollector _outputCollector;
    private Map<String, Selection> selections;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
        selections = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        TableRowUpd tableUpdate = null;
        String viewName = null;

        switch (tuple.getSourceStreamId()) {
            case (WhereBolt.UPDATE_DISTRIBUTION_STREAM):
                tableUpdate = (TableRowUpd) tuple.getValueByField("update");
                viewName = (String) tuple.getValueByField("viewName");

                if (!viewName.equals("")) {
                    if (selections.containsKey(viewName))
                        selections.get(viewName).processUpdate(tableUpdate, viewName);
                } else
                    for (Map.Entry<String, Selection> selectionByViewName : selections.entrySet()) {
                        Selection selection = selectionByViewName.getValue();
                        viewName = selectionByViewName.getKey();
                        selection.processUpdate(tableUpdate, viewName);
                    }

                _outputCollector.emit(tuple, new Values(tableUpdate));
                break;

            case (ViewCollectionBolt.VIEW_SELECTION_DISTRIBUTION_STREAM):
                viewName = (String) tuple.getValueByField("viewName");
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
