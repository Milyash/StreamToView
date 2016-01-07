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
import table.tableupdate.TableUpd;
import view.Where;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by milya on 19.12.15.
 */
public class WhereBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(WhereBolt.class);
    public static final String BOLT_NAME = "Where Bolt";
    public static final String UPDATE_DISTRIBUTION_STREAM = "WhereBolt: Update distribution stream";
    private OutputCollector _outputCollector;
    private HashMap<String, Where> wheres;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
        wheres = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        switch (tuple.getSourceStreamId()) {
            case (ViewCollectionBolt.UPDATE_DISTRIBUTION_STREAM):

                TableRowUpd rowUpdate = (TableRowUpd) tuple.getValueByField("update");
                LOG.error("======================WHERE: " + rowUpdate);
                if (rowUpdate == null) {
                    _outputCollector.ack(tuple);
                    return;
                }
                if (wheres.size() == 0) {
                    _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values("", rowUpdate));
                } else {
                    for (Map.Entry<String, Where> whereByTableName : wheres.entrySet()) {
                        String viewName = whereByTableName.getKey();
                        Where where = whereByTableName.getValue();
                        boolean passFurther = false;
//                        LOG.error(" 00000000000000000000000 upd: " + rowUpdate + ", where: ");
                        if (where == null || !rowUpdate.getTableName().equals(where.getField().getTableName()))
                            passFurther = true;
                        else
                            passFurther = where.processUpdate(rowUpdate, viewName);

                        if (passFurther)
                            _outputCollector.emit(UPDATE_DISTRIBUTION_STREAM, tuple, new Values(viewName, rowUpdate));
                    }
                }
                break;


            case (ViewCollectionBolt.VIEW_WHERE_DISTRIBUTION_STREAM):
                String viewName = (String) tuple.getValueByField("viewName");
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
        outputFieldsDeclarer.declareStream(WhereBolt.UPDATE_DISTRIBUTION_STREAM, new Fields("viewName", "update"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
