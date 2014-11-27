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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GetLongitudeBolt implements IBasicBolt{
	
	private static final long serialVersionUID = 1L;
	private HashMap<String,String> longitude=new HashMap<String,String>();
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
        return con; //返回所建立的数据库连接  
    }  

    public static void insert(String area,String jing,String wei) {          
        conn = getConnection(); // 首先要获取连接，即连接到数据库   
        try {  
            String sql = "INSERT INTO position(area,jing,wei)"  
                    + " VALUES ('"+area+"','"+jing+"','"+wei+"')";  // 插入数据的sql语句                
            st = (Statement) conn.createStatement();    // 创建用于执行静态sql语句的Statement对象                
            st.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数                
            //System.out.println("向position表中插入 " + count + " 条数据"); //输出插入操作的处理结果              
            conn.close();   //关闭数据库连接                
        } catch (SQLException e) {  
            System.out.println("插入数据失败" + e.getMessage());  
        }  
    }  
    
	
	
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.toString();
		
        if(longitude.get(word)!=null){
        	insert(longitude.get(word).split("\t",-1)[0],longitude.get(word).split("\t",-1)[1],longitude.get(word).split("\t",-1)[2]);
        }
	}
    
	
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("area", "jing","wei"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		conn = getConnection();
		try {
			st = (Statement) conn.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}  
		String uri = "hdfs://master:9000/storm/jingweidu.txt";
		InputStream in = null;		
		Configuration conf2 = new Configuration();
		
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(uri), conf2);
			in = fs.open(new Path(uri));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			while (null != (line = br.readLine())) {
				longitude.put(line.split("\t", -1)[0], line);			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
}

