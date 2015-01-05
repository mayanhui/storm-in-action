package chaptor0203.bolt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import chaptor0203.utils.CalculateCache;


public class CalcluateLessThanBolt  extends BaseRichBolt{
	private static final long serialVersionUID = -2297748406092200728L;

	private OutputCollector collector;
	
	CalculateCache cache;
	
	//设定超时时间
	int timeSection;
	//计算线程描述
	String desc;
	
	public CalcluateLessThanBolt(int timeSection, String desc){
		this.timeSection = timeSection;
		this.desc = desc;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		cache = new CalculateCache(timeSection, new CalcluateLessThanBoltExpiredCallback());
		
	}

	@Override
	public void execute(Tuple input) {
		cache.put(input.getIntegerByField("lt"));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
	
	private class CalcluateLessThanBoltExpiredCallback implements CalculateCache.ExpiredCallback{

		@Override
		public void expire(AtomicInteger currentValue) {
			int finalResult = currentValue.getAndSet(0);
			System.out.printf("%s的%d分钟内数据个数为%d; %s\n", new Object[]{desc, timeSection, finalResult, System.currentTimeMillis()/1000});
		}
		
	}
	

}

