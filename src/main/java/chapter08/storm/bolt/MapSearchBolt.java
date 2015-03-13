package chapter08.storm.bolt;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import chapter08.gis.GPSRcrd;
import chapter08.gis.RoadGridList;

/**
 * realODMatrix realODMatrix.bolt DistrictMatchingBolt.java
 * 
 */
public class MapSearchBolt implements IRichBolt {

	private static final long serialVersionUID = -433427751113113358L;

	// private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	Integer roadID = null;
	GPSRcrd record;
	Map<GPSRcrd, Integer> gpsMatch; // map<GPSRcrd,roadID>
	Integer taskID;
	String taskname;
	List<Object> inputLine;
	Fields matchBoltDeclare = null;

	static String path = "/data/gis/SZRoad_new2.shp";
	public static RoadGridList sects = null;
	static int count1 = 0;
	static int count2 = 0;
	public static int count3 = 0;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		this.taskID = context.getThisTaskId();
		this.taskname = context.getThisComponentId();

	}

	public void execute(Tuple input) {
		count1++;
		try {
			if (sects == null) {
				count3++;
				sects = new RoadGridList(path);
			}
			// System.out.println("District Match input:"+input.toString());
			// FieldListenerSpout.writeToFile("mapBoltInput",input.toString());

			List<Object> inputLine = input.getValues();// getFields();

			String speed = (String) inputLine.get(3);
			if (speed.equals("0"))
				return;

			record = new GPSRcrd(Double.parseDouble((String) inputLine.get(6)),
					Double.parseDouble((String) inputLine.get(5)),
					Integer.parseInt((String) inputLine.get(3)),
					Integer.parseInt((String) inputLine.get(4)));

			if (Double.parseDouble((String) inputLine.get(6)) > 114.5692938
					|| Double.parseDouble((String) inputLine.get(6)) < 113.740000
					|| Double.parseDouble((String) inputLine.get(5)) > 22.839945
					|| Double.parseDouble((String) inputLine.get(5)) < 22.44)
				return;

			// roadID = sects.fetchSect(record);

			roadID = sects.fetchRoadID(record);

			if (roadID != -1) {
				count2++;
				System.out.print("[" + count1 + ":" + count2 + ":" + count3
						+ "]: GPS falls Road No. :" + roadID);
				// FieldListenerSpout.writeToFile("roadID","GPS Point falls into Sect No. :"+roadID.toString()+"\n");

				inputLine.add(Integer.toString(roadID));
				// input.getFields().toList().add("roadID");
				List<String> fieldList = input.getFields().toList();
				fieldList.add("roadID");
				matchBoltDeclare = new Fields(fieldList);
				// FieldListenerSpout.writeToFile("/home/ghchen/output","matchBoltDeclare="+matchBoltDeclare);

				String[] obToStrings = new String[inputLine.size()];
				obToStrings = inputLine.toArray(obToStrings);
				// for(int i=0;i<obToStrings.length-1;i++)
				// FieldListenerSpout.writeToFile("/home/ghchen/map-oput",obToStrings[i]+",");
				// FieldListenerSpout.writeToFile("/home/ghchen/map-oput","\n");

				// System.out.print("[ Emit success:"+ count + "]");
				_collector.emit(new Values(obToStrings));
				// _collector.emit(new Values(inputLine));
			}

		} catch (Exception e) {

			e.printStackTrace();
		}

		_collector.ack(input);

	}

	public void cleanup() {
		// TODO Auto-generated method stub

		System.out.println("-- District Mathchier [" + taskname + "-" + roadID
				+ "] --");
		for (Map.Entry<GPSRcrd, Integer> entry : gpsMatch.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("viechleID", "dateTime", "occupied",
				"speed", "bearing", "latitude", "longitude", "roadID"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}