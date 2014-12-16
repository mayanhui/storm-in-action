package chapter06.kafka.producer.tail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class MutliTailProducer implements LogFileTailerListener{
	static Logger log= Logger.getLogger(DirectoryMoniter.class);
	private Map<String,String> topicMapping;
	private Producer<String, String> producer;
	private LogFileTailer tailer;
	private Set<String> keySet;
	private int nullTopic;
	private Thread t;
	
    public MutliTailProducer(String filename,String zkconnect,Map<String,String> topicMapping,int runtime,long interval) {
    	int curtime = (int)(System.currentTimeMillis()/1000);
		Properties props = new Properties();
		props.put("zk.connect",zkconnect);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		this.producer = new Producer<String, String>(config); 
		this.topicMapping = new HashMap<String,String>();
		this.topicMapping.putAll(topicMapping);
		this.keySet = this.topicMapping.keySet();
		this.nullTopic = 0;
        tailer = new LogFileTailer(new File(filename), interval, true,curtime + runtime);
        tailer.addLogFileTailerListener(this);
        this.t = new Thread(tailer);
        this.t.start();
    }	
	
	@Override
	public void newLogFileLine(String line) {
		// TODO Auto-generated method stub
		String topic = this.findTopic(line);
		if (topic != null){
			ProducerData<String, String> data = new ProducerData<String, String>(topic, line);
			this.producer.send(data);			
		} else {
			this.nullTopic++;
			//log.info("match failed: log is [" + line + "]");
		}
	}
	
	private String findTopic(String line){
		String topic = null;
		for (String keyword:this.keySet){
			if (line.contains(keyword)){
				topic = this.topicMapping.get(keyword).toString();
				break;
			}
		}
		return topic;
	}
	
	@Override
    public void close(){
    	log.info("null topic num is " + this.nullTopic);
    	this.producer.close();
    }	
}
