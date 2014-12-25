package chapter09.storm.adpv;

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
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import net.sf.json.JSONObject;
//import backtype.storm.tuple.Values;

//20140117        03      183.19.140.101  2014-01-17 03:44:51     {EFBA4702-B9B2-929A-B76D-BC4AA4E84942}  5.32.1227.1111  716     917     {"wid":"13","aid":"101853","vid":"1450739","adid":"32540","asid":"1","aspid":"1","mid":"16771","ptime":"","mg":"15,16,43,71,93,107,148,153,159,160,162,170,173,183,193","ag":"4,20,28,104,157,213,1869,1909","ecode":"0","type":"1","dpl":"0","adpid":"0","dsp":"0"}    中国    广东省  肇庆市  NULL

public class AdpvUpdate implements IBasicBolt {

	public Configuration config;
	public HTable table;
//	public HBaseAdmin admin;

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
			// table = new HTable(config,
			// Bytes.toBytes("user_behavior_attribute_noregistered"));
			table = new HTable(config, Bytes.toBytes("realtime_adpv_stat"));
//			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String rowkey = tuple.getString(0);
		long amount = tuple.getLong(1);

		String family = "pv";
		String qualifier = "cnt";
		//
		try {
			long count = table.incrementColumnValue(Bytes.toBytes(rowkey),
					Bytes.toBytes(family), Bytes.toBytes(qualifier), amount);
			System.out.println("[AdpvUpdate]Current count: " + count);
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
		// TODO Auto-generated method stub
		return null;
	}

}
