package chapter08.storm.bolt;

import java.sql.*;

public class SQLManager {

	private Connection conn = null;
	private Statement st = null;
	private ResultSet rs = null;

	public SQLManager(String host, String port, String databaseName,
			String userName, String password) {
		try {
			// 写入驱动所在处，打开驱动
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			// 数据库，用户，密码，创建与具体数据库的连接
			conn = DriverManager.getConnection("jdbc:mysql://" + host + ":"
					+ port + "/" + databaseName, userName, password);
			// 创建执行sql语句的对象
			st = conn.createStatement();

		} catch (Exception e) {
			System.out.println("连接失败" + e.toString());

		}
	}

	public String select(String sqlStatement) {
		String result = new String();
		int size = 0;
		try {
			rs = st.executeQuery(sqlStatement);
			size = st.getResultSet().getMetaData().getColumnCount();
			while (rs != null && rs.next()) {
				for (int i = 0; i < size; i++) {
					result = result + rs.getString(i + 1) + ",";
				}
				result = result + "\n";
				// result=rs.getString(n);
				// 列的记数是从1开始的，这个适配器和C#的不同
			}

			rs.close();
			return result;
		} catch (Exception e) {
			System.out.println("查询失败" + e.toString());

			return null;
		}
	}

	public int query(String sqlStatement) {
		int row = 0;
		try {
			row = st.executeUpdate(sqlStatement);
			return row;
		} catch (Exception e) {
			System.out.println("执行sql语句失败" + e.toString());
			return row;
		}
	}

	public void close() {
		try {
			if (rs != null)
				this.rs.close();
			if (st != null)
				this.st.close();
			if (conn != null)
				this.conn.close();

		} catch (Exception e) {
			System.out.println("关闭数据库连接失败" + e.toString());
		}
	}

	public static void main(String[] args) throws SQLException {
		String ip = "192.168.170.10";

		SQLManager mysql = new SQLManager(ip, "3306", "realOD",
				"ghchen", "ghchen");

		mysql = new SQLManager(ip, "3306", "realTimeTraffic",
				"ghchen", "ghchen");
		String s = mysql.select("select * from realTimeTraffic.roadSpeed;");
		System.out.println(s);

		String nowtime = "2013-04-12 14:28:30";
		String roadId = "10000";
		int avgSpd = 1;
		int count = 1;
		int rs = mysql
				.query("insert into realTimeTraffic.roadSpeed(time,roadID,speed,count) values('"
						+ nowtime
						+ "','"
						+ roadId
						+ "',"
						+ avgSpd
						+ ","
						+ count + " );");

		mysql.query("delete from realTimeTraffic.roadSpeed");
		rs = mysql
				.query("insert into realTimeTraffic.roadSpeed(time,roadID,speed,count) values('"
						+ nowtime
						+ "','"
						+ roadId
						+ "',"
						+ avgSpd
						+ ","
						+ count + " );");
		System.out.println(rs);

	}

}
