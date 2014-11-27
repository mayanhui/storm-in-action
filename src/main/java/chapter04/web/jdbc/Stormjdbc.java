package chapter04.web.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import chapter04.web.bean.StormBean;
import chapter04.web.service.StormService;


public class Stormjdbc {

	/**
	 * @param args
	 * ��ݿ����
	 */
	private static String url="jdbc:mysql://localhost:3306/test";
    private static String user="root";
    private static String password="1";
    public static Connection getConnection(){
    	Connection conn=null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
    	
		return conn;
    	
    }
    
    
    //����ȫ�����
    public static List<StormBean> findAll(){
    	
    	Connection conn=getConnection();
		PreparedStatement ps=null;
		ResultSet rs=null;
		String sql="select * from storm ";
		List<StormBean> list=new ArrayList<StormBean>();
		try {
			ps=conn.prepareStatement(sql);
			rs=ps.executeQuery();
			while(rs.next()){
				StormBean sto=new StormBean();;
			    sto.setLng(rs.getDouble("lng"));
			    sto.setLat(rs.getDouble("lat"));
			    sto.setAddress(rs.getString("address"));
				list.add(sto);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(ps!=null){
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(rs!=null){
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return list;
    }

}
