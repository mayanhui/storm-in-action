package chaptor0203.bolt;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentence implements IBasicBolt{
	@Override
    public void prepare(Map conf, TopologyContext context) {
     }
	@Override
   public void execute(Tuple tuple, BasicOutputCollector collector) {
          String sentence = tuple.getString(0);
           for(String word: sentence.split(" ")) {
                    collector.emit(new Values(word));
              }
         }
	@Override
     public void cleanup() {
    }
     @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
         }
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
