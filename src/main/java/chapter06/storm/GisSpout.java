package chapter06.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class GisSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;

	// Random _rand;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		// _rand = new Random();
	}

	@Override
	public void nextTuple() {
		// String[] sentences = new String[] { "the cow jumped over the moon",
		// "an apple a day keeps the doctor away",
		// "four score and seven years ago",
		// "snow white and the seven dwarfs", "i am at two with nature" };
		// String sentence = sentences[_rand.nextInt(sentences.length)];
		// _collector.emit(new Values(sentence));

		String uri = "hdfs://master:9000/gps/car";
		InputStream in = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			// IOUtils.copyBytes(in, System.out, 4096, false);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				_collector.emit(new Values(line));
				Utils.sleep(100);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("gis_lk"));
	}

}