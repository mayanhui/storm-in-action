package chapter09.storm.adclick;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import chapter09.util.ConfigFactory;
import chapter09.util.ConfigProperties;

public class AdclickUpdate implements IBasicBolt {

	public Configuration config;
	public HTable table;

	ConfigProperties cp = ConfigFactory.getInstance().getConfigProperties(
			ConfigFactory.APP_CONFIG_PATH);

	public void prepare(Map conf, TopologyContext context) {
		config = HBaseConfiguration.create();
		config.set("hbase.master",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_MASTER));
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set(
				"hbase.zookeeper.quorum",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM));
		try {
			table = new HTable(config, Bytes.toBytes("realtime_adclick_stat"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String rowkey = tuple.getString(0);
		long amount = tuple.getLong(1);

		String family = "clk";
		String qualifier = "cnt";
		//
		try {
			long count = table.incrementColumnValue(Bytes.toBytes(rowkey),
					Bytes.toBytes(family), Bytes.toBytes(qualifier), amount);
			System.out.println("[AdclickUpdate]Current count: " + count);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void cleanup() {
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowkey", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
