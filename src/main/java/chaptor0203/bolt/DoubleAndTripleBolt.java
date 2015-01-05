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
 * 
 */
public class DoubleAndTripleBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	@Override
	public void execute(Tuple input) {
		int val = input.getInteger(0);
		_collector.emit(input, new Values(val * 2, val * 3));
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("double", "triple"));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub

	}

}
