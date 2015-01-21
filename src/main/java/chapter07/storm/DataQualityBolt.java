package chapter07.storm;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

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

@SuppressWarnings("serial")
public class DataQualityBolt implements IBasicBolt {
	public Configuration config;
	public HTable tableCnt;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowkey", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", "master:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		try {
			tableCnt = new HTable(config, Bytes.toBytes("cellphone_cnt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String value[] = input.getString(0).split("\t", -1);
		String newtime = TransferTime(value[0]);
		String cellphonenumber = value[1];
		System.out.println("s1-->" + newtime + " " + cellphonenumber);
		Put put = new Put(Bytes.toBytes(newtime));
		// boolean abnormal = false;
		long currentcnt = -1l;
		long emportynumber = 1l;
		try {

			currentcnt = tableCnt.incrementColumnValue(Bytes.toBytes(newtime),
					Bytes.toBytes("cf1"), Bytes.toBytes("cnt"), 1L);
			System.out.println("s2-->" + currentcnt + "  "
					+ Long.toString(currentcnt));
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("totalnum"),
					Bytes.toBytes(Long.toString(currentcnt)));
			try {
				tableCnt.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (cellphonenumber.equals("")) {
			try {
				emportynumber = tableCnt.incrementColumnValue(
						Bytes.toBytes(newtime), Bytes.toBytes("cf1"),
						Bytes.toBytes("emptynumber"), 1L);
				System.out.println("s3-->" + emportynumber + "  "
						+ Long.toString(emportynumber));

				put.add(Bytes.toBytes("cf1"), Bytes.toBytes("emptynum"),
						Bytes.toBytes(Long.toString(emportynumber)));
				tableCnt.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void cleanup() {

		try {
			tableCnt.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String TransferTime(String time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		sdf.setTimeZone(TimeZone.getTimeZone("Asia/ShangHai"));
		String sd = sdf.format(new Date(Long.parseLong(time)));

		return sd;
	}
}
