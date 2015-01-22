package chapter0203.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import chapter0203.bolt.CalcluateGeThanBolt;
import chapter0203.bolt.CalcluateLessThanBolt;
import chapter0203.bolt.ClassifyBolt;
import chapter0203.spout.RandomSpout;

public class TopologyBuilderDemo {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		// 设置spout的worker为1，task为1

		builder.setSpout("randomSpout", new RandomSpout(), 1).setNumTasks(1);
		// 设置分类bolt的worker为3，task为3，既是每个worker分配1个task
		builder.setBolt("classifyBolt", new ClassifyBolt(), 3).setNumTasks(3)
				.shuffleGrouping("randomSpout");

		// 随机大于等于50的数据从classifyBolt-->geThan中取出
		builder.setBolt("calcluateGeThanBolt",
				new CalcluateGeThanBolt(1, "随机大于等于50"), 1).setNumTasks(1)
		// .shuffleGrouping("classifyBolt", "geThan");
				.fieldsGrouping("classifyBolt", "geThan", new Fields("gt"));

		// 随机小于50的数据从classifyBolt-->lessThan中取出
		builder.setBolt("calcluateLessThanBolt",
				new CalcluateLessThanBolt(1, "随机小于50"), 1).setNumTasks(1)
		// .shuffleGrouping("classifyBolt", "lessThan");
				.fieldsGrouping("classifyBolt", "lessThan", new Fields("lt"));

		Config conf = new Config();
		conf.setMaxTaskParallelism(3);
		conf.setDebug(true);
		conf.setNumAckers(0);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-oneMinite", conf,
				builder.createTopology());

		Thread.sleep(60 * 1000);

	}

}
