package chapter08.storm.spout;

import java.io.Serializable;

import backtype.storm.tuple.Fields;

public class TupleInfo implements Serializable {
	private static final long serialVersionUID = 8524559685781732841L;
	private String viechleID;
	private String dateTime;
	private Integer occupied;
	private Integer speed;
	private Integer bearing;
	private Double latitude;
	private Double longitude;

	private int numMember = 7;

	public TupleInfo() {
		viechleID = "";
		dateTime = "";
		occupied = 0;
		speed = 0;
		bearing = 0;
		latitude = 0.0;
		longitude = 0.0;

	}

	public TupleInfo(String[] input) {
		viechleID = input[0];
		dateTime = input[1];
		occupied = Integer.parseInt(input[2]);
		speed = Integer.parseInt(input[3]);
		bearing = Integer.parseInt(input[4]);
		latitude = Double.parseDouble(input[5]);
		longitude = Double.parseDouble(input[6]);

	}

	public int getTupleLength() {
		return numMember;
	}

	public Fields getFieldList() {
		Fields fieldList = new Fields("viechleID", "dateTime", "occupied",
				"speed", "bearing", "latitude", "longitude");
		return fieldList;
	}

	public static String getDelimiter() {
		return ",";

	}
}