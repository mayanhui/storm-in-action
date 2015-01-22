package chapter0203.topology;

import java.util.ArrayList;
import java.util.List;

import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import chapter0203.bolt.SplitSentence;
import chapter0203.bolt.WordCountBolt;

public class WordCountTopology {
	public static void main(String[] args) {
		List<String> hosts = new ArrayList<String>();
		hosts.add("test-1");
		hosts.add("test-2");
		hosts.add("test-3");

		SpoutConfig kafkaConf = new SpoutConfig(StaticHosts.fromHostString(
				hosts, 10), "storm-sentence", "", "storm");
		kafkaConf.scheme = new StringScheme();
		kafkaConf.zkPort = 2181;
		kafkaConf.forceStartOffsetTime(-2); // To start from beginning of topic

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("1", new KafkaSpout(kafkaConf), 10);// id, spout,
																// parallelism_hint
		builder.setBolt("2", new SplitSentence(), 10).shuffleGrouping("1");
		builder.setBolt("3", new WordCountBolt(), 20).fieldsGrouping("2",
				new Fields("word"));

		Config config = new Config();
		config.setDebug(true);
		try {
			StormSubmitter.submitTopology("sentence_word_count", config,
					builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	}
}
