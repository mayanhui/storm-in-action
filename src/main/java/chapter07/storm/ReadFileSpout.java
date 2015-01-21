package chapter07.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class ReadFileSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		String uri = "hdfs://master:9000/cellPhoneApp";
		Configuration conf = new Configuration();
		InputStream in = null;
		try {
			FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(uri));
			Path[] listPath = FileUtil.stat2Paths(fs);
			for (Path p : listPath) {
				String filepath = p.toString();
				FileSystem fs1 = FileSystem.get(URI.create(filepath), conf);
				in = fs1.open(new Path(filepath));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String line = null;
				while (null != (line = br.readLine())) {
					String arr[] = line.split("\t", -1);
					String value = arr[0] + "\t" + arr[1];
					_collector.emit(new Values(value));
					System.out.println("spout" + " " + line);
					Utils.sleep(100);
				}
				Path path = new Path(filepath);
				FileSystem fs2 = path.getFileSystem(conf);
				fs2.delete(path, true);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
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
		declarer.declare(new Fields("cellphone"));
	}

}
