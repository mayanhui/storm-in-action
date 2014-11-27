package chapter06.web.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import chapter06.web.model.Car;



public class JDBCConnect {
	
	private static char[] list;
	ResultSet rs = null;
	Statement stmt = null;
	Connection coon = null;
	
	
	public void getConntion() throws ClassNotFoundException, SQLException {
		System.out.println("1111111");
		String url = "jdbc:mysql://localhost:3306/carstatus?useUnicode=true&characterEncoding=UTF-8";
		String user = "root";
		String password = "123456";
		Class.forName("com.mysql.jdbc.Driver");
		coon = DriverManager.getConnection(url, user, password);
		System.out.println("111");
		if (coon == null) {
			System.out.println("!!!!!!!!!!1");

		}

	}

	
	
	public void closeConn() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
			if (coon != null) {
				coon.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	public List get(String sql) throws SQLException,
			ClassNotFoundException {
		getConntion();
		stmt = coon.createStatement();
		rs = stmt.executeQuery(sql);
		List list = new ArrayList();
		while (rs.next()) {
			int DirectId = rs.getInt(1);
			int count = rs.getInt(2);
			Car c = new Car(DirectId, count);
			list.add(c);
		}
		closeConn();
		return list;

	}

}
