package chaptor0203.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author csm
 * 从spout中获的数据后 以50为界，将数据分发到两个stream流中：geThan和lessThan
 * 
 */
public class ClassifyBolt extends BaseRichBolt{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3327014892951144349L;

	//分类标识，大于等于50的放在一起，小于50的放在一起
	private static final int CLASSIFY_FLAG = 50;
	
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	
	
	@Override
	public void execute(Tuple input) {
		int randomInt = input.getIntegerByField("randomInt");
//		大于等于50的放在一起
		if(randomInt >= CLASSIFY_FLAG){
			collector.emit("geThan", new Values(randomInt));
		}else{
//			小于50的放在一起
			collector.emit("lessThan",new Values(randomInt));
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//在geThan流中声明为gt
		declarer.declareStream("geThan", new Fields("gt"));
		//在lessThan流中声明为lt
		declarer.declareStream("lessThan", new Fields("lt"));
	}

}
