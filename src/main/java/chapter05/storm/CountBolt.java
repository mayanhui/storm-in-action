package chapter05.storm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class CountBolt extends BaseBasicBolt {

	Integer id;
	String name;

	public PreparedStatement prepStatement;
	private Connection connection;
	private String url = "jdbc:mysql://192.168.29.32:3306/check_account";
	private String password = "123456";
	private String driver = "com.mysql.jdbc.Driver";
	private String username = "zkpk";
	private PreparedStatement st = null;

	Map<String, Integer> counters;

	@Override
	public void cleanup() {

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		try {
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			connection = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		String str = input.getString(0);
		System.out.println("Received: " + str);

		try {
			String[] all = str.split("\t", -1);
			String uid = all[0];
			String date = all[1];
			int times = Integer.parseInt(all[2]);
			int hour = Integer.parseInt(all[3]);
			String tag = all[4];
			String sql = "insert into abnormal_user(uid,date,times,hour,tag) values(?,?,?,?,?)";
			st = connection.prepareStatement(sql);

			st.setString(1, uid);
			st.setString(2, date);
			st.setInt(3, times);
			st.setInt(4, hour);

			st.setString(5, tag);
			st.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
