package chapter04.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class GetLongitudeBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;
	private HashMap<String, String> longitude = new HashMap<String, String>();
	static Connection conn;
	static Statement st;

	private String uri = "hdfs://master:9000/storm/lng-lat-mapping.txt";

	public static Connection getConnection() {
		Connection con = null; // get connection
		try {
			Class.forName("com.mysql.jdbc.Driver");// load Mysql driver
			con = DriverManager.getConnection(
					"jdbc:mysql://192.168.32.72:3306/test", "hadoop", "hadoop");
		} catch (Exception e) {
			System.out.println("connect mysql failed! " + e.getMessage());
		}
		return con; // return connection
	}

	public static void insert(String area, String jing, String wei) {
		conn = getConnection(); // get connection
		try {
			String sql = "INSERT INTO position(area,lng,lat)" + " VALUES ('"
					+ area + "','" + jing + "','" + wei + "')"; 
			st = (Statement) conn.createStatement(); // create static sql statement
			st.executeUpdate(sql); // exec sql 
			conn.close(); // close connection
		} catch (SQLException e) {
			System.out.println("insert failed! " + e.getMessage());
		}
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.toString();

		if (longitude.get(word) != null) {
			insert(longitude.get(word).split("\t", -1)[0], longitude.get(word)
					.split("\t", -1)[1], longitude.get(word).split("\t", -1)[2]);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("area", "lng", "lat"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		conn = getConnection();
		try {
			st = (Statement) conn.createStatement();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		InputStream in = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(uri), new Configuration());
			in = fs.open(new Path(uri));
		} catch (IOException e) {
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			while (null != (line = br.readLine())) {
				longitude.put(line.split("\t", -1)[0], line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void cleanup() {
	}

}
