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
import table.tableupdate.TableRowDeleteUpd;
import table.tableupdate.TableRowUpd;
import view.Having;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by milya on 19.12.15.
 */
public class HavingBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HavingBolt.class);
    private static final String LOG_STRING = "------ HavingBolt: ";

    public static final String UPDATE_DISTRIBUTION_STREAM = "HavingBolt: Update distribution stream";

    public OutputCollector _outputCollector;
    private HashMap<String, Having> havings;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
        havings = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String viewName = (String) tuple.getValueByField("viewName");
        switch (tuple.getSourceStreamId()) {

            case (AggregatesBolt.UPDATE_DISTRIBUTION_STREAM):

                TableRowUpd update = (TableRowUpd) tuple.getValueByField("update");

                String logString = " get: viewName: " + viewName + "; update: " + update;

                LOG.error(LOG_STRING + logString);

                if (update == null) {
                    _outputCollector.ack(tuple);
                    return;
                }
                if (havings.get(viewName) == null) {
                    LOG.error(LOG_STRING + " no havings => pass of" + logString);
                    _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(viewName, update));
                } else {
                    Having having = havings.get(viewName);
                    boolean passFurther;

                    if (having == null)
                        passFurther = true;
                    else
                        passFurther = having.processUpdate(update, viewName);

                    if (passFurther == false)
                        update = new TableRowDeleteUpd(update.getTableName(), update.getPk());

                    LOG.error(LOG_STRING + " having " + having + " is Met => pass of " + logString);
                    _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(viewName, update));
                }
                break;

            case (ViewCollectionBolt.VIEW_HAVING_DISTRIBUTION_STREAM):
                Having having = (Having) tuple.getValueByField("havings");
                havings.put(viewName, having); // todo or totally replace???
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
