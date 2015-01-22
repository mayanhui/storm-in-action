package chapter0203.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class WordCountBolt extends BaseRichBolt {

	private static Logger logger = Logger.getLogger(WordCountBolt.class);

	private Connection connection;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		connect();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		String sql = "insert into t_word (word) values ('" + word + "')";
		Statement stat = null;
		try {
			stat = this.connection.createStatement();
			stat.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void cleanup() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private Connection connect() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			logger.error(e);
		}
		Connection conn = null;
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://192.168.205.224:3306/databus",
							"root", "root");
			this.connection = conn;
		} catch (SQLException e) {
			logger.error(e);
		}
		return conn;
	}

}
