package chapter09.storm.adpv;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

public class AdpvMain {

	public static void main(String[] args) throws Exception {

		List<String> hosts = new ArrayList<String>();
		hosts.add("data-test-208");
		hosts.add("data-test-210");
		hosts.add("data-test-211");

		SpoutConfig kafkaConf = new SpoutConfig(StaticHosts.fromHostString(
				hosts, 10), "adpv-test-1", "/kafkastorm", "adpv");
		kafkaConf.scheme = new StringScheme();
		kafkaConf.zkPort = 2181;
		kafkaConf.forceStartOffsetTime(-2); // To start from beginning of topic
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 4);
		builder.setBolt("combined-rk", new AdpvCombinedRowkey())
				.shuffleGrouping("spout");
		builder.setBolt("cnt", new AdpvUpdate()).shuffleGrouping(
				"combined-rk");

		Config config = new Config();
		// config.setDebug(true);
		config.setNumWorkers(4);
		config.setMaxSpoutPending(1000);
		StormSubmitter.submitTopology("adpv-topology", config,
				builder.createTopology());

	}
}
