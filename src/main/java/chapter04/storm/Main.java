package chapter04.storm;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class Main {	

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new ReaderSpout(), 4);
		//4 the number of tasks that should be assigned to execute this spout
		builder.setBolt("area-bolt", new GetAreaBolt()).shuffleGrouping("spout");
		builder.setBolt("longitude-bolt", new GetLongitudeBolt()).shuffleGrouping("area-bolt");

		Config config = new Config();
		config.setNumWorkers(4);
		config.setMaxSpoutPending(1000);
		StormSubmitter.submitTopology("area-topology", config,
				builder.createTopology());

	}
}
