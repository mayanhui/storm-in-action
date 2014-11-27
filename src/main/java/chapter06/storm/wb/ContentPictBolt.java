package net.kafka.consumer.wb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.kafka.consumer.util.ConfigFactory;
import net.kafka.consumer.util.ConfigProperties;
import net.kafka.consumer.util.JsonUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//[{"beCommentWeiboId":"","beForwardWeiboId":"3654376063078681","catchTime":"1387159495","commentCount":"2684","content":"谁能帮忙联系上？我供养五仟元救急！","createTime":"1386758088","info1":"","info2":"","info3":"","mlevel":"","musicurl":[],"pic_list":[],"praiseCount":"7961","reportCount":"6104","source":"iPad客户端","userId":"1087770692","videourl":[],"weiboId":"3654390646794785","weiboUrl":"http://weibo.com/1087770692/AmOWQsvDj"}]

/**
 * 
 * (1)微博内容为空的 (2)微博内容中存在“赶紧下载”内容的 (3)附带图片数量超过9张的
 * 
 * @author zkpk
 * 
 */
public class ContentPictBolt implements IBasicBolt {

	public JsonUtil jsonUtil;
	public Configuration config;
	public HTable table;
	// public HBaseAdmin admin;
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
			table = new HTable(config, Bytes.toBytes("weibo_abnormal"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getString(0).trim();
		line = line.substring(1, line.length() - 1);

		String content = jsonUtil.evaluate(line, "$.content").toString();
		String uid = jsonUtil.evaluate(line, "$.userId").toString();
		boolean abnormal = false;
		// (1)
		if (null == content || content.trim().length() == 0) {
			abnormal = true;
		}
		// (2)
		if (content.indexOf("赶紧下载") >= 0) {
			abnormal = true;
		}

		// (3)
		String pic = jsonUtil.evaluate(line, "$.pic_list").toString();
		if (null != pic && pic.length() > 2) {
			pic = pic.substring(1, pic.length() - 1);
			String[] arr = pic.split(",", -1);
			if (arr.length > 9)
				abnormal = true;
		}

		if (!abnormal) {
			collector.emit(new Values(uid, line));
		} else {
			Put put = new Put(Bytes.toBytes(uid));
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("wb"),
					Bytes.toBytes(line));
			try {
				table.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void cleanup() {
		if (null != table)
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uid", "ts"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public static void main(String[] args) throws Exception {
		String line = "[{\"beCommentWeiboId\":\"\",\"beForwardWeiboId\":\"3654376063078681\",\"catchTime\":\"1387159495\",\"commentCount\":\"2684\",\"content\":\"谁能帮忙联系上？我供养五仟元救急！\",\"createTime\":\"1386758088\",\"info1\":\"\",\"info2\":\"\",\"info3\":\"\",\"mlevel\":\"\",\"musicurl\":[],\"pic_list\":[\"http://ww2.sinaimg.cn/thumbnail/52e0e4f8jw1ebfwa7tpg0j208c069t9b.jpg\"],\"praiseCount\":\"7961\",\"reportCount\":\"6104\",\"source\":\"iPad客户端\",\"userId\":\"1087770692\",\"videourl\":[],\"weiboId\":\"3654390646794785\",\"weiboUrl\":\"http://weibo.com/1087770692/AmOWQsvDj\"}]";
		line = line.substring(1, line.length() - 1);
		JsonUtil jsonUtil = new JsonUtil();
		String content = jsonUtil.evaluate(line, "$.content").toString();
		System.out.println(content);

		String uid = jsonUtil.evaluate(line, "$.userId").toString();
		System.out.println(uid);

		String pic = jsonUtil.evaluate(line, "$.pic_list").toString();
		System.out.println(pic);

		List<String> picList = jsonUtil.evaluateArray(line, "$.pic_list");
		System.out.println(picList.size());
	}

}
