package chapter09.storm.adclick;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//20140117	23	123.170.97.43	2014-01-17 23:00:00	{C9A51C40-3B9F-A29F-2BE3-72CC50E88
//DF9}	5.32.1227.1111	964	917	{"wid":"13","aid":"8322","vid":"434872","adid":"32623","as
//id":"1","aspid":"1","mid":"16868","ptime":"1","mg":"15,39,153,160,170,173,190,193","ag":"4,20,29,1
//01,213,1869,1909","area":"26","dpl":"0","adpid":"0","dsp":"0","channel":""}	中国	山东省	日
//照市	NULL

public class AdClickCombinedRowkey implements IBasicBolt {

	public void prepare(Map conf, TopologyContext context) {

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getString(0);
		String[] arr = line.split("\t", -1);
		if (arr.length > 10) {
			System.out
					.println("------------------[AdClickCombinedRowkey]-------------");
			String date = arr[0].trim();
			String json = arr[8].trim();

			if (null != json && json.trim().length() > 0) {
				String adid = null;
				Pattern p = Pattern.compile("\\\"adid\\\":\\\".*?\\\"");
				Matcher mc = p.matcher(json);
				while (mc.find()) {
					adid = mc.group();
				}

				if (null == adid) {
					System.out.println("Invalid json: " + json);
					return;
				}

				p = Pattern.compile("\\d+");
				mc = p.matcher(adid);
				while (mc.find()) {
					adid = mc.group();
				}

				if (null != adid && adid.trim().length() > 0
						&& !adid.trim().equals("NULL") && null != date
						&& date.length() > 0 && !date.trim().equals("NULL")) {

					String rowkey = adid + "_" + date;
					collector.emit(new Values(rowkey, 1L));
					System.out.println("[AdClickCombinedRowkey]rk-province: "
							+ rowkey);

				}
			}
		}
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowkey", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
