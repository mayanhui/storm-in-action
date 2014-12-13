package chapter05.web.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import chapter05.web.model.DataSet;

public class JDBCUtils {
	private static final String url = "jdbc:mysql://192.168.29.32:3306/project?user=zkpk"
			+ "&password=123456";
	// uid abnormalTime times abnormal_account
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection() {
		try {
			return DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Map<Integer, List<DataSet>> getData(Date d) {
		Map<Integer, List<DataSet>> maps = new TreeMap<Integer, List<DataSet>>();
		List<DataSet> dss = null;
		PreparedStatement pst = null;
		Connection conn = getConnection();
		ResultSet rs = null;
		try {
			pst = conn
					.prepareStatement("select hour,count(distinct uid),times from abnormal_account where createTime=? group by hourse,times ");
			pst.setDate(1, new java.sql.Date(new Date().getTime()));
			rs = pst.executeQuery();
			while (rs.next()) {
				if (maps.get(rs.getInt(1)) == null) {
					dss = new ArrayList<DataSet>();
					maps.put(rs.getInt(1), dss);
				}
				DataSet ds = new DataSet();
				ds.setTimes(rs.getInt(3));
				ds.setCount(rs.getInt(2));
				dss.add(ds);
			}
		} catch (SQLException e) {

			e.printStackTrace();
		}
		return maps;
	}

	public static void insertData() {
		PreparedStatement pst = null;
		Connection conn = getConnection();
		Random r = new Random();
		try {
			for (int j = 1; j <= 12; j++) {
				int t = r.nextInt(20) + 20;

				for (int i = 0; i < t; i++) {
					pst = conn
							.prepareStatement("insert into abnormal_account values(?,?,?,?)");
					pst.setString(1, "128762431" + i);
					pst.setDate(2, new java.sql.Date(new Date().getTime()));
					pst.setInt(3, 02);
					pst.setInt(4, j);
					pst.executeUpdate();
				}
			}
			pst.close();
			conn.close();

		} catch (SQLException e) {

			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		insertData();
		Map<Integer, List<DataSet>> maps = getData(new Date());
		System.out.println(maps.size());
		for (Integer key : maps.keySet()) {
			// System.out.println(key);
			for (DataSet ds : maps.get(key)) {
				System.out.println(key + "=" + ds.getTimes() + "="
						+ ds.getCount());
			}
		}
	}
}