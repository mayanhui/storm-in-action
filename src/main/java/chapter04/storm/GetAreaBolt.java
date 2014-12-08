package chapter04.storm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class GetAreaBolt implements IBasicBolt {

	static Connection conn;
	static Statement st;

	public static Connection getConnection() {
		Connection con = null; // define Connection
		try {
			Class.forName("com.mysql.jdbc.Driver");// load Mysql driver
			con = DriverManager.getConnection(
					"jdbc:mysql://192.168.32.72:3306/test", "hadoop", "hadoop");
		} catch (Exception e) {
			System.out.println("Connection failed! " + e.getMessage());
		}
		return con;

	}

	public static String select(long ipp) {
		conn = getConnection(); // get connection
		try {
			String sql = "select area from ip where '" + ipp
					+ "' between minip and maxip";
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(sql);
			String name = rs.getString("area");
			return name;
			// conn.close(); //close connection
		} catch (SQLException e) {
			System.out.println("failed! " + e.getMessage());
			return null;
		}

	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.toString();
		String all[] = line.split("\t", -1);
		long longIp = GetAreaBolt.ipToLong(all[3]);
		collector.emit(new Values(select(longIp)));
	}

	public static long ipToLong(String strIp) {
		// transfer ip like 127.0.0.1 to decimal integer
		long[] ip = new long[4];
		// find the position of dot
		int position1 = strIp.indexOf(".");
		int position2 = strIp.indexOf(".", position1 + 1);
		int position3 = strIp.indexOf(".", position2 + 1);
		// transfer string to integer
		ip[0] = Long.parseLong(strIp.substring(0, position1));
		ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
		ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
		ip[3] = Long.parseLong(strIp.substring(position3 + 1));
		return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("area"));
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
			st = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
	}

}
