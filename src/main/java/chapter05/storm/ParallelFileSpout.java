package chapter05.storm;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

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

@SuppressWarnings("serial")
public class ParallelFileSpout extends BaseRichSpout {

	public int last;
	public SpoutOutputCollector collector;
	public InputStream in;
	public AppConfiguration conf1 = new AppConfiguration();
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
	public Date d = new Date();
	public String dir1 = sdf.format(d);
	public String dir = dir1.substring(0, 10);
	public String hour = dir1.substring(11, 13);
	@SuppressWarnings("rawtypes")
	public Map conf;
	FileSystem fs;

	String uri = "hdfs://192.168.29.32:9000/flume/" + dir;
	private boolean completed = false;

	/**
	 * when successfully process topology, send ack. then send fail.
	 */
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);

	}

	public void close() {
	}

	/**
	 * when successfully process topology, send ack. then send fail.
	 */
	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	/**
	 * called in SpoutTracker. called once, send a single tuple.
	 */
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			return;
		}
		read();

	}

	public void read() {
		String str;
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		try {
			while ((str = reader.readLine()) != null) {
				String str1 = str + "\t" + dir + "\t" + last + "\t" + hour;
				collector.emit(new Values(str1));
			}
			collector.emit(new Values("over"));
			Thread.sleep(30000);
			if (last != getFilesNum()) {
				getIn();
				read();
			}

		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date d = new Date();
		String dir = sdf.format(d);
		String uri = "hdfs://192.168.29.32:9000/flume/data/" + dir;
		try {
			fs = FileSystem.get(URI.create(uri), conf1);
			FileStatus[] status = fs.listStatus(new Path(uri));
			Path[] listedPaths = FileUtil.stat2Paths(status);

			for (int i = 0; i < listedPaths.length; i++) {
				if (i == listedPaths.length - 1) {
					last = i;
					in = fs.open(new Path(listedPaths[i].toString()));
				}
			}

			// this.fileReader = new
			// FileReader(conf.get("wordsFile").toString());
		} catch (Exception e) {
			throw new RuntimeException("Error reading file ["
					+ conf.get("wordFile") + "]");
		}
		// this.filePath = conf.get("wordsFile").toString();
		this.collector = collector;
		this.conf = conf;
	}

	public InputStream getIn() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date d = new Date();
		String dir = sdf.format(d);
		String uri = "hdfs://192.168.29.32:9000/flume/data/" + dir;
		try {
			fs = FileSystem.get(URI.create(uri), conf1);
			FileStatus[] status = fs.listStatus(new Path(uri));
			Path[] listedPaths = FileUtil.stat2Paths(status);

			for (int i = 0; i < listedPaths.length; i++) {
				if (i == listedPaths.length - 1) {
					last = i;

					return in = fs.open(new Path(listedPaths[i].toString()));
				}
			}

		} catch (Exception e) {
			throw new RuntimeException("Error reading file ["
					+ conf.get("wordFile") + "]");
		}
		return in;
	}

	public int getFilesNum() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date d = new Date();
		String dir = sdf.format(d);
		int len = 0;
		String uri = "hdfs://192.168.29.32:9000/flume/" + dir;
		try {
			fs = FileSystem.get(URI.create(uri), conf1);
			FileStatus[] status = fs.listStatus(new Path(uri));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			len = listedPaths.length - 1;

		} catch (Exception e) {

		}
		return len;
	}

	/**
	 * define field. used for grouping by field.
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word1"));
	}

}
