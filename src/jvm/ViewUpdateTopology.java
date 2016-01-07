import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.slf4j.LoggerFactory;

/**
 * Created by Milya on 20.05.2015.
 */
public class ViewUpdateTopology {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ViewUpdateTopology.class);
    private static Config config;

    private static void initLocalConfig() {
        config = new Config();
        config.setDebug(true);
    }

    private static void initRemoteConfig() {
        config = new Config();
        config.setDebug(true); // should be false

        config.setNumWorkers(1);
        config.setMaxSpoutPending(300);
    }

    private static void remoteSubmit(StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        StormSubmitter.submitTopology("MyTopology", config, topology);
    }

    private static void localSubmit(StormTopology topology) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("operationTopology", config, topology);
        Utils.sleep(500000);
        cluster.killTopology("operationTopology");
        cluster.shutdown();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        StormTopology topology = ViewTopology.build();

        initLocalConfig();
        localSubmit(topology);

//        initRemoteConfig();
//        remoteSubmit(topology);

    }
}
