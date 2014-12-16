package chapter06.kafka.producer.tail;

import java.io.File;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class TailProducer implements LogFileTailerListener {
	/**
	 * The log file tailer
	 */
	private LogFileTailer tailer;
	private Producer<String, String> producer;
	private String topic;
	private Thread t;

	/**
	 * Creates a new Tail instance to follow the specified file
	 */
	public TailProducer(String filename, String zkconnect, String topic,
			int runtime, long interval) {
		int curtime = (int) (System.currentTimeMillis() / 1000);
		Properties props = new Properties();
		props.put("zk.connect", zkconnect);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.sessiontimeout.ms", String.valueOf(300000));
		// props.put("producer.type", "async");
		props.put("buffer.size", String.valueOf(64 * 1024));
		props.put("reconnect.interval", String.valueOf(Integer.MAX_VALUE));
		props.put("compression.codec", 2);
		ProducerConfig config = new ProducerConfig(props);
		this.producer = new Producer<String, String>(config);
		this.topic = topic;
		tailer = new LogFileTailer(new File(filename), interval, true, curtime
				+ runtime);
		tailer.addLogFileTailerListener(this);
		this.t = new Thread(tailer);
		this.t.start();
		// tailer.start();
	}

	/**
	 * A new line has been added to the tailed log file
	 * 
	 * @param line
	 *            The new line that has been added to the tailed log file
	 */
	public void newLogFileLine(String line) {
		ProducerData<String, String> data = new ProducerData<String, String>(
				this.topic, line);
		this.producer.send(data);
	}

	public boolean getState() {
		return this.t.isAlive();
	}

	@Override
	public void close() {
		this.producer.close();
	}
}
