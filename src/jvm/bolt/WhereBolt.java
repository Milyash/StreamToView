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
import view.Where;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by milya on 19.12.15.
 */
public class WhereBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WhereBolt.class);
    private static final String LOG_STRING = "------ WhereBolt: ";

    public static final String UPDATE_DISTRIBUTION_STREAM = "WhereBolt: Update distribution stream";

    public OutputCollector _outputCollector;
    private HashMap<String, Where> wheres;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
        wheres = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String viewName = (String) tuple.getValueByField("viewName");
        switch (tuple.getSourceStreamId()) {

            case (ViewCollectionBolt.UPDATE_DISTRIBUTION_STREAM):

                TableRowUpd update = (TableRowUpd) tuple.getValueByField("update");

                String logString = " get: viewName: " + viewName + "; update: " + update;

                LOG.error(LOG_STRING + logString);

                if (update == null) {
                    _outputCollector.ack(tuple);
                    return;
                }
                if (wheres.get(viewName) == null) {
                    LOG.error(LOG_STRING + " no wheres => pass of" + logString);
                    _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(viewName, update));
                } else {
                    Where where = wheres.get(viewName);
                    boolean passFurther;

                    if (where == null)
                        passFurther = true;
                    else
                        passFurther = where.processUpdate(update, viewName);

                    if (passFurther == false)
                        update = new TableRowDeleteUpd(update.getTableName(), update.getPk());

                    LOG.error(LOG_STRING + " where " + where + " is Met => pass of " + logString);
                    _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(viewName, update));
                }
                break;

            case (ViewCollectionBolt.VIEW_WHERE_DISTRIBUTION_STREAM):
                Where where = (Where) tuple.getValueByField("wheres");
                wheres.put(viewName, where); // todo or totally replace???
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
