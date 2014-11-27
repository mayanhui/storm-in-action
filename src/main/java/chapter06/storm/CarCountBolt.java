package chapter06.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import chapter06.storm.dao.JDBC;
import chapter06.storm.util.ConfigFactory;
import chapter06.storm.util.ConfigProperties;
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
public class CarCountBolt implements IBasicBolt {

	static HashMap<String, Integer> map = new HashMap();
	static JDBC jdbc = new JDBC();
	public Configuration config;
	public HTable tableDistrict;
	// public HTable tableAbnormal;
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
			tableDistrict = new HTable(config, Bytes.toBytes("car_district"));
			// tableAbnormal = new HTable(config,
			// Bytes.toBytes("weibo_abnormal"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		String uri = "hdfs://master:9000/gis/dlxx.txt";
		InputStream in = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), config);
			in = fs.open(new Path(uri));
			// IOUtils.copyBytes(in, System.out, 4096, false);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				String[] arr = line.split(",", -1);
				if (arr.length > 2) {
					map.put(new Double(arr[1]) * 2 + "," + new Double(arr[2])
							* 2, new Integer(arr[0]));
				}
				System.out.println("cytlsw" + arr.length);
				System.out.println(line);

			}
			System.out.println("hsbzsc" + map.size());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}

		for (int n = 1; n < 401; n++) {
			// System.out.println("1234567890+_)(*&^%$#@!~");
			try {
				jdbc.set(n, 0);
				System.out.println("cytmysql");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// String uid = tuple.getString(0);

		String line = tuple.getString(0);

		// gen date
		String carId = line.split(",", -1)[0];

		int oldId = 0;

		Get get = new Get(Bytes.toBytes(carId));
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("id"));
		Result res = null;
		try {
			res = tableDistrict.get(get);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// String id =
		// Bytes.toString(tableDistrict.get(get).list().get(0).getValue());
		if (res.size() != 0) {
			oldId = Bytes.toInt(res.list().get(0).getValue());
		}

		String jing = String.format("%.1f",
				new Double(line.split(",", -1)[1]) * 2);
		String wei = String.format("%.1f",
				new Double(line.split(",", -1)[2]) * 2);

		if (String.format("%.2f", new Double(line.split(",", -1)[1]) * 2)
				.compareTo(jing) < 0) {
			jing = String.format("%.1f",
					new Double(line.split(",", -1)[1]) * 2 - 0.1);
		}
		if (String.format("%.2f", new Double(line.split(",", -1)[2]) * 2)
				.compareTo(wei) < 0) {
			wei = String.format("%.1f",
					new Double(line.split(",", -1)[2]) * 2 - 0.1);
		}

		int newId = map.get(jing + "," + wei);

		Put put = new Put(Bytes.toBytes(carId));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("id"), Bytes.toBytes(newId));
		try {
			tableDistrict.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (oldId != newId) {

			int count = 0;
			try {
				count = jdbc.get(oldId);
				jdbc.update(oldId, count - 1);
				count = jdbc.get(newId);
				jdbc.update(newId, count + 1);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public void cleanup() {
		try {
			tableDistrict.close();
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
