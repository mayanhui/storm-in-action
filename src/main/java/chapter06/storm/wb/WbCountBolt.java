package net.kafka.consumer.wb;

import java.io.IOException;
import java.util.Map;

import net.kafka.consumer.util.ConfigFactory;
import net.kafka.consumer.util.ConfigProperties;
import net.kafka.consumer.util.DateFormatUtil;
import net.kafka.consumer.util.JsonUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import net.sf.json.JSONObject;

//[{"beCommentWeiboId":"","beForwardWeiboId":"3654376063078681","catchTime":"1387159495","commentCount":"2684","content":"谁能帮忙联系上？我供养五仟元救急！","createTime":"1386758088","info1":"","info2":"","info3":"","mlevel":"","musicurl":[],"pic_list":[],"praiseCount":"7961","reportCount":"6104","source":"iPad客户端","userId":"1087770692","videourl":[],"weiboId":"3654390646794785","weiboUrl":"http://weibo.com/1087770692/AmOWQsvDj"}]

/**
 * 
 * (4)1天内发送微博数量超过100的
 * 
 * @author zkpk
 * 
 */
public class WbCountBolt implements IBasicBolt {

	public JsonUtil jsonUtil;

	public Configuration config;
	public HTable tableCnt;
	public HTable tableAbnormal;
	ConfigProperties cp = ConfigFactory.getInstance().getConfigProperties(
			ConfigFactory.APP_CONFIG_PATH);

	public void prepare(Map conf, TopologyContext context) {
		jsonUtil = new JsonUtil();
		config = HBaseConfiguration.create();
		config.set("hbase.master",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_MASTER));
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set(
				"hbase.zookeeper.quorum",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM));
		try {
			tableCnt = new HTable(config, Bytes.toBytes("weibo_cnt"));
			tableAbnormal = new HTable(config, Bytes.toBytes("weibo_abnormal"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String uid = tuple.getString(0);
		String line = tuple.getString(1);

		// gen date
		long ts = Long.parseLong(jsonUtil.evaluate(line, "$.createTime")
				.toString());
		String date = DateFormatUtil.parseToStringDate(ts);

		long currentCnt = -1L;
		try {
			currentCnt = tableCnt.incrementColumnValue(
					Bytes.toBytes(uid + "_" + date), Bytes.toBytes("cf1"),
					Bytes.toBytes("cnt"), 1L);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if (currentCnt > 100) {
			Put put = new Put(Bytes.toBytes(uid));
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("wb"),
					Bytes.toBytes(line));
			try {
				tableAbnormal.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void cleanup() {
		try {
			tableAbnormal.close();
			tableCnt.close();
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
