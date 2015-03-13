package chapter08.storm.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DataBaseLoadBolt implements IRichBolt {

	SQLManager mysql = null;

	public class Road {
		Date nowDate;
		String roadID;
		int speed;
		int count;

		Road() {
		}

		Road(Date nowDate, String roadID, int speed, int count) {
			this.nowDate = nowDate;
			this.roadID = roadID;
			this.speed = speed;
			this.count = count;
		}
	}

	ArrayList<Road> szRoads = new ArrayList<Road>();
	Map<String, Road> roadMap = new HashMap<String, Road>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		List<Object> inputRoad = input.getValues();
		String roadID = (String) inputRoad.get(1);
		Road road = new Road((Date) inputRoad.get(0),
				(String) inputRoad.get(1), (Integer) inputRoad.get(2),
				(Integer) inputRoad.get(3));
		roadMap.put(roadID, road);

		if (mysql == null)
			mysql = new SQLManager("172.20.36.247", "3306", "realTimeTraffic",
					"ghchen", "ghchen");
		Date nowDate = new Date();
		@SuppressWarnings("deprecation")
		int second = nowDate.getSeconds();
		if ( /* (min%2) ==0 && */(second == 0)) {

			mysql.query("delete from realTimeTraffic.roadSpeed");
			DataBaseLoadBolt.writeToMysql(mysql, roadMap);
		}

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public static void writeToMysql(SQLManager mysql, Map<String, Road> roadMap) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String nowtime;
		for (Entry<String, Road> entry : roadMap.entrySet()) {
			nowtime = sdf.format(entry.getValue().nowDate);
			int rs = mysql
					.query("insert into realTimeTraffic.roadSpeed(time,roadID,speed,count) values('"
							+ nowtime
							+ "','"
							+ entry.getValue().roadID
							+ "',"
							+ entry.getValue().speed
							+ ","
							+ entry.getValue().count + " );");
			if (rs != 0)
				System.out.println("Insert into Mysql success :   "
						+ entry.getValue().roadID + "',"
						+ entry.getValue().speed);
		}

	}
}
