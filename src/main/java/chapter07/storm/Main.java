package chapter07.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class Main {
	public static void main(String args[]) throws AlreadyAliveException,
			InvalidTopologyException {
		// List<String> hosts=new ArrayList<String>();
		// hosts.add("master1");
		// SpoutConfig spoutConf = new
		// SpoutConfig(StaticHosts.fromHostString(hosts,
		// 10),"cellphone-test1","/cellphone","cellphone");
		// spoutConf.zkPort=2181;
		// spoutConf.forceStartOffsetTime(-2);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new ReadFileSpout(), 1);
		builder.setBolt("DataQuality", new DataQualityBolt()).shuffleGrouping(
				"spout");
		Config config = new Config();
		config.setNumWorkers(4);
		config.setMaxSpoutPending(1000);
		StormSubmitter.submitTopology("DataQuality-topology", config,
				builder.createTopology());

	}

}
