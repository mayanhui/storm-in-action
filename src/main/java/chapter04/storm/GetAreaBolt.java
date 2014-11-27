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

public class GetAreaBolt implements IBasicBolt{
	
	static Connection conn; 
	static Statement st;
	
	/* 获取数据库连接的函数*/  
    public static Connection getConnection() {  
        Connection con = null;  //创建用于连接数据库的Connection对象  
        try {  
            Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动                
            con = DriverManager.getConnection("jdbc:mysql://192.168.32.72:3306/test", "hadoop", "hadoop");// 创建数据连接,hadoop is user and password               
        } catch (Exception e) {  
            System.out.println("数据库连接失败" + e.getMessage());  
        }
		return con;  
        
    }
    public static String select(long ipp) {          
    	conn = getConnection(); // 首先要获取连接，即连接到数据库 
        try {  
            String sql = "select area from ip where '"+ipp+"' between minip and maxip";   
         st = conn.createStatement();            
            ResultSet rs=st.executeQuery(sql);  
            String name=rs.getString("Name") ;
            return name;
            //conn.close();   //关闭数据库连接                
        } catch (SQLException e) {  
            System.out.println("失败" + e.getMessage()); 
            return null;
        }
		
		  
    }  
    
    @Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.toString();
		String all[]=line.split("\t",-1);
		//System.out.println("###" + all.length + "###");
		long longIp = GetAreaBolt.ipToLong(all[3]);
		collector.emit(new Values(select(longIp)));
	}
	
    
	public static long ipToLong(String strIp){//将127.0.0.1形式的IP地址转换成十进制整数
        long[] ip = new long[4];
        //先找到IP地址字符串中.的位置
        int position1 = strIp.indexOf(".");
        int position2 = strIp.indexOf(".", position1 + 1);
        int position3 = strIp.indexOf(".", position2 + 1);
        //将每个.之间的字符串转换成整型
        ip[0] = Long.parseLong(strIp.substring(0, position1));
        ip[1] = Long.parseLong(strIp.substring(position1+1, position2));
        ip[2] = Long.parseLong(strIp.substring(position2+1, position3));
        ip[3] = Long.parseLong(strIp.substring(position3+1));
        return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
    }
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("area"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		 conn = getConnection(); // 首先要获取连接，即连接到数据库   
		 try {
			 st = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // 创建用于执行静态sql语句的Statement对象   
	}
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
