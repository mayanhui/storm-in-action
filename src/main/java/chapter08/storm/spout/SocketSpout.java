package chapter08.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

public class SocketSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
	String[] GPSRecord;
	private String IP = "192.168.170.10";
	static Socket sock = null;

	@Override
	public void close() {

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

	}

	@Override
	public void nextTuple() {

		OutputStream output = null;
		String encryptText = "siatxdata\r\ngps\r\n";
		int ch = 0;
		try {
			if (sock == null) {
				sock = new Socket(IP, 5557);
				output = sock.getOutputStream();
				output.write(encryptText.getBytes());
				output.flush();
			}
			while (true) {
				byte[] b3 = new byte[3];
				if (sock != null) {
					try {
						sock.getInputStream().read(b3, 0, 3);
						ch = b3[0];
					} catch (Exception e) {
						System.out
								.println("connection reset, reconnecting ...");
						sock.close();
						Thread.sleep(10000);
						sock = new Socket(IP, 5557);
						output = sock.getOutputStream();
						output.write(encryptText.getBytes());
						output.flush();
					}

				} else {
					sock = new Socket(IP, 5557);
					output = sock.getOutputStream();
					output.write(encryptText.getBytes());
					output.flush();
					break;
				}
				int len = SocketJava.bytesToShort(b3, 1);
				if (len < 0)
					break;
				byte[] bytelen = new byte[len];
				sock.getInputStream().read(bytelen);
				sock.getInputStream().markSupported();
				sock.getInputStream().mark(3);

				String gpsString = SocketJava.DissectOneMessage(ch, bytelen);
				String[] GPSRecord = null;
				if (gpsString != null) {
					GPSRecord = gpsString.split(TupleInfo.getDelimiter());

					_collector.emit(new Values(GPSRecord[0], GPSRecord[3],
							GPSRecord[7], GPSRecord[5], GPSRecord[6],
							GPSRecord[2], GPSRecord[1]));
				} else {
					break;
				}

			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void ack(Object id) {
		System.out.println("OK:" + id);
	}

	@Override
	public void fail(Object id) {
		System.out.println("Fail:" + id);
	}

	@Override
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

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
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
