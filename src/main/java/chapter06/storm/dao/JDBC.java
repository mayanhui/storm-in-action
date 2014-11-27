package chapter06.storm.dao;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBC {

	java.sql.PreparedStatement pstm=null;
	Statement stmt = null;
	Connection coon = null;
	ResultSet rs=null;

	public void getConntion() throws ClassNotFoundException, SQLException {
		//?useUnicode=true&characterEncoding=UTF-8"
		String url = "jdbc:mysql://192.168.29.232:3306/GpsDb";
		String user = "hadoop";
		String password = "hadoop";
		Class.forName("com.mysql.jdbc.Driver");
		coon = DriverManager.getConnection(url, user, password);
		System.out.println(coon);

	}
//	public static void main(String[] args) throws ClassNotFoundException, SQLException{
//		JDBC jdbc=new JDBC();
//		jdbc.getConntion();
//	}
	public void closeConn() {
		try {
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
	
	public int get(int id) throws SQLException, ClassNotFoundException {
		getConntion();
		stmt = coon.createStatement();
		rs = stmt.executeQuery("select count from car where DirectID="+id);
		int count=rs.getInt(0);
		
		closeConn();
		return count;

	}
	
	
	public void set(int id,int num) throws SQLException, ClassNotFoundException {
		getConntion();
		
		pstm=coon.prepareStatement("insert into car(DirectID,ccount) values("+id+","+num+")");
		pstm.executeUpdate();
		System.out.println("lswmysql");
		closeConn();

	}

	public void update(int id,int num) throws SQLException, ClassNotFoundException {
		getConntion();
		
		pstm=coon.prepareStatement("update car set ccount="+num+" where DirectID="+id);
		pstm.executeUpdate();

		closeConn();

	}

}
