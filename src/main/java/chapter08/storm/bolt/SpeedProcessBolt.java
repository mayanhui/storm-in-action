package chapter08.storm.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import chapter08.gis.FixedSizeQueue;

public class SpeedProcessBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	double lanLast; // last location of the vehicle
	double lonLast;
	Date dateTimeLast = null;
	int INTERVAL0 = 120; // We set time windows between two points 120 seconds;
	double DIST0 = 0.008993; // On the Earth, 1 Degree =111.2 km
								// Distance between two points 1km, shoule be
								// 1/111.2 =0.008993 Degree;

	private OutputCollector _collector;
	Integer taskId;
	String taskName;
	// Map<String, List<String> > Roads; //RoadID, vehicleIdsInThisArea
	public static LinkedList<Road> Roads = new LinkedList<Road>();
	// static public List<String> vehicleIdsInThisArea=new ArrayList<String>();
	Integer cnt;
	Timer timer;
	SQLManager mysql = null;

	public class spdList extends ArrayList<Integer> {
		private static final long serialVersionUID = 1L;
		Integer speed;

		spdList() {
		}

		spdList(Integer speed) {
			this.speed = speed;
		};
	}

	public class Road {
		public Road() {
		}

		public Road(String roadID, FixedSizeQueue<Integer> roadSpd) {
			this.roadId = roadID;
			this.roadSpd = roadSpd;

		}

		public String roadId;
		public int count;// 计算次数，是车牌号的个数码
		// public Date dateTime; //该路线统计的车辆出现时间
		FixedSizeQueue<Integer> roadSpd;
		int avgSpd;
		// public HashMap<String,spdList> roadSpd;
		// //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
		// public HashMap<String,String> vieLngLatIDList;
		// //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
	}

	public Road getRoadById(String RoadId) {
		for (Road d : Roads) {
			if (d.roadId.equals(RoadId)) {
				return d;
			}
		}
		return null;
	}

	public int getAvgById(String RoadId) {
		for (Road d : Roads) {
			if (d.roadId.equals(RoadId)) {
				return d.avgSpd;
			}
		}
		return -1;
	}

	public int getCountById(String RoadId) {
		for (Road d : Roads) {
			if (d.roadId.equals(RoadId)) {
				return d.count;
			}
		}
		return -1;
	}

	public Boolean isDisExits(List<Road> Roads, String RoadId) {
		for (Road d : Roads) {
			if (d.roadId.equals(RoadId)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.taskName = context.getThisComponentId();
		this.taskId = context.getThisTaskId();
		this._collector = collector;
	}

	BufferedWriter br;
	int count = 0;

	@SuppressWarnings("null")
	@Override
	public void execute(Tuple input) {

		String roadID = input.getValues().get(7).toString();
		// double lan =
		// Double.parseDouble(input.getValues().get(5).toString());// lan
		// double lon = Double.parseDouble(input.getValues().get(6).toString());
		// //lon
		// String viechId = input.getValues().get(0).toString();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Integer speed = Integer.parseInt(input.getValues().get(3).toString());
		Date dateTime = null;
		int averageSpeed = 0;
		int count = 0;
		// try {
		// dateTime = sdf.parse(input.getValues().get(1).toString());
		//
		// } catch (ParseException e1) {
		// e1.printStackTrace();
		// }

		if (!isDisExits(Roads, roadID)) {
			// 没有此路线，则新建一个路径，并存起来
			Road road = new Road();
			/* FixedSizeQueue<Integer> */
			road.roadSpd = new FixedSizeQueue<Integer>(30);
			road.roadSpd.add(speed);

			road.roadId = roadID;
			road.count = 1;
			// road.roadSpd=road.roadSpd;

			road.avgSpd = speed;

			Roads.add(road); // 添加路线
			averageSpeed = speed;
			count = 1;

		} else { // 如果已经有该路线

			Road road = getRoadById(roadID);
			// if(!Road.roadSpd.contains(viechId)){
			// //但是如果车辆ID是第一次进入该区域，新建一个车辆ID，并保存；

			int sum = 0;
			if (road.roadSpd.size() < 2) {
				road.count++;
				road.roadSpd.add(speed);

				for (Integer it : road.roadSpd) {
					sum = sum + it;
				}
				road.avgSpd = (int) ((double) sum / (double) road.roadSpd
						.size());
				averageSpeed = road.avgSpd;
				count = road.roadSpd.size();
			} else {
				System.out.print("RoadID3  ");
				double avgLast = getAvgById(roadID);

				double temp = 0;
				// FieldListenerSpout.writeToFile("SpeedList", RoadID+":");
				for (Integer it : road.roadSpd) {
					// FieldListenerSpout.writeToFile("SpeedList", it+",");
					sum = sum + it;
					temp += Math.pow((it - avgLast), 2);
				}
				// FieldListenerSpout.writeToFile("SpeedList", "\n");
				int avgCurrent = (int) ((sum + speed) / ((double) road.roadSpd
						.size() + 1));
				temp = (temp + Math.pow((speed - avgLast), 2))
						/ (road.roadSpd.size());
				double standdev = Math.sqrt(temp);
				if (Math.abs(speed - avgCurrent) <= 2 * standdev) {
					road.count++;
					road.roadSpd.add(speed);
					road.avgSpd = avgCurrent;
					averageSpeed = avgCurrent;
					count = road.roadSpd.size();
					// System.out.println("\n\naverage speed:"+road.count+":"+road.avgSpd+"\n\n");
				}
			}
		}
		Date nowDate = new Date();
		_collector.emit(new Values(nowDate, roadID, averageSpeed, count));

		_collector.ack(input);

	}

	@Override
	public void cleanup() {
		System.out.println("-- Real Time Traffic [" + taskName + "-" + taskId
				+ "] --");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nowDate", "RoadID", "averageSpeed",
				"count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	static class Job extends java.util.TimerTask {
		@Override
		public void run() {

		}
	}

	public static void writeToFile(String fileName, LinkedList<Road> Roads) {
		try {
			String[] name = fileName.split("/");
			String tmp = null;
			if (name[name.length - 1].length() > 13) {
				tmp = fileName.substring(0, fileName.length() - 6);
			} else {
				tmp = fileName;
			}
			BufferedWriter br = new BufferedWriter(new FileWriter(tmp, true));
			// BufferedWriter br = new BufferedWriter(new
			// FileWriter(fileName,true));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
			String nowtime = sdf.format(new Date());
			// ddRoad=Roads;
			for (Road d : Roads) {
				// br.write(d.RoadId+","+d.count+"#"+d.RoadSpd.values()+";"+
				// d.vieLngLatIDList.values()+"\n");
				br.write("\n" + nowtime + "," + d.roadId + "," + d.avgSpd + ","
						+ d.roadSpd.size());
				br.flush();
				System.out.print(nowtime + "," + d.roadId + "," + d.avgSpd
						+ "," + d.roadSpd.size() + "\n");
			}

			br.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static void writeToMysql(SQLManager mysql, LinkedList<Road> Roads) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String nowtime = sdf.format(new Date());
		for (Road road : Roads) {
			int rs = mysql
					.query("insert into realTimeTraffic.roadSpeed(time,roadID,speed,count) values('"
							+ nowtime
							+ "','"
							+ road.roadId
							+ "',"
							+ road.avgSpd + "," + road.count + " );");
			if (rs != 0)
				System.out.println("Insert into Mysql success :   "
						+ road.roadId + "'," + road.avgSpd);
		}

	}

	public static void newFolder(String folderPath) {
		try {
			String filePath = folderPath.toString();
			java.io.File myFilePath = new java.io.File(filePath);
			if (!myFilePath.exists()) {
				myFilePath.mkdir();
			}
		} catch (Exception e) {
			System.out.println("Eorror: Can't create new folder!");
			e.printStackTrace();
		}
	}
}