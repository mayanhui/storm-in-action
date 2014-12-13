package chapter05;

import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import chapter05.storm.AppConfiguration;

public class ForFile {

	public InputStream in;
	public int last;
	AppConfiguration conf1=new AppConfiguration();
	public InputStream getIn() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
    		 Date d=new Date();
    	    String dir=sdf.format(d);
    	    String uri="hdfs://192.168.29.32:9000/flume/data/"+dir;
        try {
        	FileSystem fs=FileSystem.get(URI.create(uri),conf1);
        	FileStatus[] status=fs.listStatus(new Path(uri));
    		Path[]listedPaths=FileUtil.stat2Paths(status);

    		for(int i=0;i<listedPaths.length;i++){
    			if(i==listedPaths.length-1){
    				last=i;
    				
    				return in=fs.open(new Path(listedPaths[i].toString()));
 
    				
    			}
    		}
        	
          //  this.fileReader = new FileReader(conf.get("wordsFile").toString());
        } catch (Exception e) {
            throw new RuntimeException("Error reading file ["+conf1.get("wordFile")+"]");
        }
		return in;
	}
	
	
	
}
