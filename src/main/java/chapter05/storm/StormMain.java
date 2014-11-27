package chapter05.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class StormMain {

	/**
	 * @param args
	 */
	//private static final long serialVersionUID = 5678586644899822143L;
	 public static void main(String[] args) throws Exception{
	        TopologyBuilder builder = new TopologyBuilder();
	     
	        builder.setSpout("word-reader",new ParallelFileSpout());
	     
	        builder.setBolt("word-normalizer", new WordNormalizer(),1).fieldsGrouping("word-reader", new Fields("word1"));
	
	        builder.setBolt("word-counter", new WordCount()).fieldsGrouping("word-normalizer", new Fields("word"));
	        Config conf = new Config();
	        
	        StormSubmitter.submitTopology("wordCounterTopology", conf, builder.createTopology());

	        
	        
	        
		}

}
