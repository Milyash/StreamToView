package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import table.tableupdate.TableUpd;
import walentry.WALEntry;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;

/**
 * Created by Milya on 20.05.2015.m97-639-038
 */
public class WALUpdatesSpout extends BaseRichSpout {
    private SpoutOutputCollector _outputCollector;
    private static Socket socket;
    private static ObjectInputStream ois;
    private static Logger LOG;
    private static boolean connectionAvailable;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("update"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _outputCollector = spoutOutputCollector;
        LOG = org.slf4j.LoggerFactory.getLogger(WALUpdatesSpout.class);
        connectionAvailable = false;
    }

    @Override
    public void nextTuple() {
        WALEntry walEntry = getTupleFromSocket();

        if (walEntry != null) {
//            LOG.error("---------- START");
            TableUpd tableUpd = new TableUpd(walEntry.getTableName(), walEntry.getDBRowList());
            LOG.error(tableUpd.toString());
            _outputCollector.emit(new Values(tableUpd));
            LOG.warn(tableUpd.toString());
        }

    }

    private static WALEntry getTupleFromSocket() {

        WALEntry tableUpdate = null;
        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(AppConfig.HOST, AppConfig.PORT));
            connectionAvailable = true;
            ois = new ObjectInputStream(socket.getInputStream());

            tableUpdate = (WALEntry) ois.readObject();
            ois.close();
            socket.close();
        } catch (ConnectException e) {
            if (connectionAvailable) {
                LOG.error("Connection closed on the server!");
                connectionAvailable = false;
            }
            tableUpdate = null;
        } catch (EOFException e) {
            if (connectionAvailable)
                LOG.error("End of file!");
            tableUpdate = null;
        } catch (IOException e) {
            if (connectionAvailable) {
                LOG.error("Connection disturbed on the server!");
                connectionAvailable = false;
            }
            tableUpdate = null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return tableUpdate;

    }

}
