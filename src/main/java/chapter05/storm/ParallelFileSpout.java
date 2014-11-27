package chapter05.storm;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ParallelFileSpout extends BaseRichSpout {
	/**
	 * 
	 */
	//private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	public int last;
	public SpoutOutputCollector collector;
 //   private FileReader fileReader;
   public InputStream in;
  public  MyConfiguration conf1=new MyConfiguration();
   public static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd-HH");
	public static Date d=new Date();
   public static String dir1=sdf.format(d);
   public static String dir=dir1.substring(0, 10);
   public static String hour=dir1.substring(11,13);
   public static Map conf;
 //  Path[]listedPaths;
   FileSystem fs;
 
    String uri="hdfs://192.168.29.32:9000/flume/"+dir;
	//FileSystem fs=FileSystem.get(URI.create(uri),conf1);
	//FileSystem fs1=FileSystem.get(URI.create(""), conf1);
    //private String filePath;
    private boolean completed = false;
    //storm在检测到一个tuple被整个topology成功处理的时候调用ack，否则调用fail。
    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
     
    }
    public void close() {}
    //storm在检测到一个tuple被整个topology成功处理的时候调用ack，否则调用fail。
    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }
 
  /*
   * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
   */
    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            return;
        }
        myReader();
       
    }
public void myReader() {
	String str;
	BufferedReader reader =new BufferedReader(new InputStreamReader(in));
	try{
	    while((str = reader.readLine()) != null){
	    	System.out.println("WordReader类 读取到一行数据："+ str);
	      //  this.collector.emit(new Values(str),str);
	       // collector.emit(new Values("key1","value1"));
	    	String str1=str+"\t"+dir+"\t"+last+"\t"+hour;
	    	collector.emit(new Values(str1));
	      // collector.emit("zrk",new Values(str1) );
	      // collector.e
	    //   collector.e
	        System.out.println("WordReader类 发射了一条数据："+ str);
	    }
	    collector.emit(new Values("over"));
	    Thread.sleep(30000);
	    System.out.println("((((((((((((((((((((((((((((((((((((((((((("+last);
	   if(last!=getFilesNum()){
		   getIn();
		   myReader();
	   }
	    
	}catch(Exception e){
	    throw new RuntimeException("Error reading tuple",e);
	}finally{
	    completed = true;
	}
}
 
    public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
    	SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		 Date d=new Date();
	    String dir=sdf.format(d);
	    String uri="hdfs://192.168.29.32:9000/flume/data/"+dir;
   try {
   	 fs=FileSystem.get(URI.create(uri),conf1);
   	FileStatus[] status=fs.listStatus(new Path(uri));
		Path[]listedPaths=FileUtil.stat2Paths(status);

		for(int i=0;i<listedPaths.length;i++){
			if(i==listedPaths.length-1){
				last=i;
				System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"+last);
				 in=fs.open(new Path(listedPaths[i].toString()));

				
			}
		}
   	
     //  this.fileReader = new FileReader(conf.get("wordsFile").toString());
   } catch (Exception e) {
       throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
   }
    //	this.filePath	= conf.get("wordsFile").toString();
        this.collector = collector;
        this.conf=conf;
    }
	public InputStream getIn() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
    		 Date d=new Date();
    	    String dir=sdf.format(d);
    	    String uri="hdfs://192.168.29.32:9000/flume/data/"+dir;
        try {
        	 fs=FileSystem.get(URI.create(uri),conf1);
        	FileStatus[] status=fs.listStatus(new Path(uri));
    		Path[]listedPaths=FileUtil.stat2Paths(status);

    		for(int i=0;i<listedPaths.length;i++){
    			if(i==listedPaths.length-1){
    				last=i;    	
    				
    				return in=fs.open(new Path(listedPaths[i].toString()));
    			}
    		}
        	
        } catch (Exception e) {
            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }
		return in;
	}
	public int getFilesNum(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		 Date d=new Date();
	    String dir=sdf.format(d);
	    int len=0;
	    String uri="hdfs://192.168.29.32:9000/flume/"+dir;
   try{
   	 fs=FileSystem.get(URI.create(uri),conf1);
   	FileStatus[] status=fs.listStatus(new Path(uri));
		Path[]listedPaths=FileUtil.stat2Paths(status);
		len= listedPaths.length-1;
		
   }catch(Exception e){
	   
   }
		return len;
	}
    /**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，该id可以用来定义更加复杂的流拓扑结构
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word1"));
    }
 
}
