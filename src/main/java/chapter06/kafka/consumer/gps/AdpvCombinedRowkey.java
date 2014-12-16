package chapter06.kafka.consumer.gps;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import net.sf.json.JSONObject;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//20140117        03      183.19.140.101  2014-01-17 03:44:51     {EFBA4702-B9B2-929A-B76D-BC4AA4E84942}  5.32.1227.1111  716     917     {"wid":"13","aid":"101853","vid":"1450739","adid":"32540","asid":"1","aspid":"1","mid":"16771","ptime":"","mg":"15,16,43,71,93,107,148,153,159,160,162,170,173,183,193","ag":"4,20,28,104,157,213,1869,1909","ecode":"0","type":"1","dpl":"0","adpid":"0","dsp":"0"}    中国    广东省  肇庆市  NULL

public class AdpvCombinedRowkey implements IBasicBolt {

	public void prepare(Map conf, TopologyContext context) {

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getString(0);
		String[] arr = line.split("\t", -1);
		if (arr.length > 10) {
			System.out
					.println("------------------[AdpvCombinedRowkey]-------------");
			String date = arr[0].trim();
			String uid = arr[4].trim();
			String json = arr[8].trim();
			String province = arr[10].trim();
			String city = arr[11].trim();

			if (null != json && json.trim().length() > 0) {
				//not use json parser due to the exception
				String adid = null;
				Pattern p = Pattern.compile("\\\"adid\\\":\\\".*?\\\"");
				Matcher mc = p.matcher(json);
				while (mc.find()) {
					adid = mc.group();
				}
				
				if(null == adid){
					System.out.println("Invalid json: " + json);
					return;
				}
				
				boolean hit = false;// if "adid":""  -> return
				p = Pattern.compile("\\d+");
				mc = p.matcher(adid);
				while (mc.find()) {
					adid = mc.group();
					hit = true;
				}
				
				if(!hit){
					System.out.println("Invalid adid: " + adid);
					return;
				}
					
				
				if (null != adid && adid.trim().length() > 0
						&& !adid.trim().equals("NULL") && null != date
						&& date.length() > 0 && !date.trim().equals("NULL")) {
					// province
					if (null != province && province.trim().length() > 0
							&& !province.trim().equals("NULL")) {
						String rowkey = adid + "_" + province + "_" + date;
						collector.emit(new Values(rowkey, 1L));
						System.out.println("[AdpvCombinedRowkey]rk-province: "
								+ rowkey);
					}
					// city
					if (null != city && city.trim().length() > 0
							&& !city.trim().equals("NULL")) {
						String rowkey = adid + "_" + city + "_" + date;
						collector.emit(new Values(rowkey, 1L));
						System.out.println("[AdpvCombinedRowkey]rk-city: "
								+ rowkey);
					}
					// uid
					if (null != uid && uid.trim().length() > 0
							&& !uid.trim().equals("NULL")) {
						String rowkey = adid + "_" + uid + "_" + date;
						collector.emit(new Values(rowkey, 1L));
						System.out.println("[AdpvCombinedRowkey]rk-city: "
								+ rowkey);
					}

				}
			}
		}

		// for (String word : sentence.split("\\s+")) {
		// collector.emit(new Values(word));
		// }
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowkey", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) {
		String json = "{\"wid\":\"13\",\"aid\":\"102197\",\"vid\":\"1423312\",\"adid\":\"32507\",\"asid\":\"1\",\"aspid\":\"0\",\"mid\":\"17263\",\"ptime\":\"15\",\"mg\":\"15,16,71,107,148,153,159,160,162,170,173,183,193,203,205\",\"ag\":\"4,20,29,101,188,213,1869,1909\",\"ecode\":\"15\",\"type\":\"2\",\"dpl\":\"0\",\"adpid\":\"0\",\"dsp\":\"0\",\"rtime\":\"47\",\"htime\":\"62\",\"dtime\":\"0\",\"swfload\":\"0\",\"swfBegt\":\"0\",\"swfEndt\":\"0\",\"writet\":\"0\",\"playt\":\"16,\"total\":\"18063\"}";
//		JSONObject jsonObj = JSONObject.fromObject(json);
//		String adid = (String) jsonObj.get("adid");
//		System.out.println(adid);
		
		String adid = null;
		Pattern p = Pattern.compile("\\\"adid\\\":\\\".*?\\\"");
		Matcher mc = p.matcher(json);
		while (mc.find()) {
			System.out.println(mc.group());
			adid = mc.group();
		}
		
		p = Pattern.compile("\\d+");
		mc = p.matcher(adid);
		while (mc.find()) {
			System.out.println(mc.group());
			adid = mc.group();
		}
		
		
//		ObjectMapper m = new ObjectMapper();
//		// can either use mapper.readTree(source), or mapper.readValue(source, JsonNode.class);
//		JsonNode rootNode = null;
//		try {
//			rootNode = m.readTree(json);
//		} catch (JsonProcessingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		// ensure that "last name" isn't "Xmler"; if is, change to "Jsoner"
//		System.out.println(rootNode.get("adid"));
////		JsonNode nameNode = rootNode.path("name");
//		String lastName = nameNode.path("last").getTextValue().
	}

}
