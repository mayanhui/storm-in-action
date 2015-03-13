package chapter08.storm.spout;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FieldListenerSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
	private BufferedReader fileReader;
	private TupleInfo tupleInfo = new TupleInfo();

	static Socket sock = null;

	public void close() {
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		String file = new String();
		if (file.equals("")) {
			file = "/tmp/gps.log";
		}

		try {

			this.fileReader = new BufferedReader(new FileReader(new File(file)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("error reading file [" + file + "]");
		}

	}

	public void nextTuple() {

		String line = null;
		BufferedReader access = new BufferedReader(fileReader);
		try {
			while ((line = access.readLine()) != null) {
				if (line != null) {
					String[] GPSRecord = line.split(tupleInfo.getDelimiter());
					// line.split("\\"+tupleInfo.getDelimiter());

					if (tupleInfo.getFieldList().size() == GPSRecord.length) {
						_collector.emit(new Values(GPSRecord));
						// tupleInfo = new TupleInfo(GPSRecord);
					}
				}
			}
		} catch (IOException ex) {
			System.out.println(ex);
		}

	}

	public void ack(Object id) {
		System.out.println("OK:" + id);
	}

	public void fail(Object id) {
		System.out.println("Fail:" + id);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		TupleInfo tuple = new TupleInfo();
		Fields fieldsArr;
		try {
			fieldsArr = tuple.getFieldList();
			declarer.declare(fieldsArr);

		} catch (Exception e) {
			throw new RuntimeException(
					"error:fail to new Tuple object in declareOutputFields, tuple is null",
					e);
		}

	}

	public void activate() {

	}

	public void deactivate() {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public static void writeToFile(String fileName, Object obj) {
		try {
			FileWriter fwriter;
			fwriter = new FileWriter(fileName, true);
			BufferedWriter writer = new BufferedWriter(fwriter);

			writer.write(obj.toString());
			// writer.write("\n\n");
			writer.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}