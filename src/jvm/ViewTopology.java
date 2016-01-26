import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import bolt.*;
import spout.WALUpdatesSpout;

/**
 * Created by milya on 10.12.15.
 */
public class ViewTopology {
    public static StormTopology build() {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("WAL entries SPOUT", new WALUpdatesSpout(), 1);
        builder.setBolt("View Collection BOLT", new ViewCollectionBolt(), 1).shuffleGrouping("WAL entries SPOUT");

        builder.setBolt("Where BOLT", new WhereBolt(), 1)
                .shuffleGrouping("View Collection BOLT", ViewCollectionBolt.VIEW_WHERE_DISTRIBUTION_STREAM)
                .shuffleGrouping("View Collection BOLT", ViewCollectionBolt.UPDATE_DISTRIBUTION_STREAM);

        builder.setBolt("Aggregate BOLT", new AggregatesBolt(), 1)
                .shuffleGrouping("View Collection BOLT", ViewCollectionBolt.VIEW_AGGREGATES_DISTRIBUTION_STREAM)
                .shuffleGrouping("Where BOLT", WhereBolt.UPDATE_DISTRIBUTION_STREAM);

        builder.setBolt("Having BOLT", new HavingBolt(), 1)
                .shuffleGrouping("View Collection BOLT", ViewCollectionBolt.VIEW_HAVING_DISTRIBUTION_STREAM)
                .shuffleGrouping("Aggregate BOLT", AggregatesBolt.UPDATE_DISTRIBUTION_STREAM);

        builder.setBolt("Selection BOLT", new SelectionBolt(), 1)
                .shuffleGrouping("View Collection BOLT", ViewCollectionBolt.VIEW_SELECTION_DISTRIBUTION_STREAM)
                .shuffleGrouping("Having BOLT", HavingBolt.UPDATE_DISTRIBUTION_STREAM);

        return builder.createTopology();
    }
}
