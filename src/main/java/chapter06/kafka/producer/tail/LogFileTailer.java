package chapter06.kafka.producer.tail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class LogFileTailer implements Runnable {
	static Logger log = Logger.getLogger(LogFileTailer.class);
	/**
	 * How frequently to check for file changes; defaults to 5 seconds
	 */
	private long sampleInterval = 5000;

	/**
	 * The log file to tail
	 */
	private File logfile;

	/**
	 * Defines whether the log file tailer should include the entire contents of
	 * the exising log file or tail from the end of the file when the tailer
	 * starts
	 */
	private boolean startAtBeginning = false;

	/**
	 * Is the tailer currently tailing?
	 */
	private boolean tailing = false;

	/**
	 * Set of listeners
	 */
	@SuppressWarnings("rawtypes")
	private Set listeners = new HashSet();

	// private int lifetime;
	private int count;

	/**
	 * Creates a new log file tailer that tails an existing file and checks the
	 * file for updates every 5000ms
	 */
	public LogFileTailer(File file) {
		this.logfile = file;
	}

	/**
	 * Creates a new log file tailer
	 * 
	 * @param file
	 *            The file to tail
	 * @param sampleInterval
	 *            How often to check for updates to the log file (default =
	 *            5000ms)
	 * @param startAtBeginning
	 *            Should the tailer simply tail or should it process the entire
	 *            file and continue tailing (true) or simply start tailing from
	 *            the end of the file
	 */
	public LogFileTailer(File file, long sampleInterval,
			boolean startAtBeginning, int lifetime) {
		this.logfile = file;
		this.sampleInterval = sampleInterval;
		this.startAtBeginning = startAtBeginning;
		// this.lifetime = lifetime;
		this.count = 0;
	}

	@SuppressWarnings("unchecked")
	public void addLogFileTailerListener(LogFileTailerListener l) {
		this.listeners.add(l);
	}

	public void removeLogFileTailerListener(LogFileTailerListener l) {
		this.listeners.remove(l);
	}

	@SuppressWarnings("rawtypes")
	protected void fireNewLogFileLine(String line) {
		for (Iterator i = this.listeners.iterator(); i.hasNext();) {
			LogFileTailerListener l = (LogFileTailerListener) i.next();
			l.newLogFileLine(line);
		}
	}

	public void stopTailing() {
		this.tailing = false;
	}

	@SuppressWarnings("rawtypes")
	private void close() {
		for (Iterator i = this.listeners.iterator(); i.hasNext();) {
			LogFileTailerListener l = (LogFileTailerListener) i.next();
			l.close();
		}
	}

	@Override
	public void run() {
		long filePointer = 0;

		if (this.startAtBeginning) {
			filePointer = 0;
		} else {
			filePointer = this.logfile.length();
		}

		try {
			this.tailing = true;
			RandomAccessFile file = new RandomAccessFile(logfile, "r");
			while (this.tailing) {
				long fileLength = this.logfile.length();
				if (fileLength < filePointer) {
					file = new RandomAccessFile(logfile, "r");
					filePointer = 0;
				}
				if (fileLength > filePointer) {
					file.seek(filePointer);
					String line = null;
					while (null != (line = file.readLine())) {
						this.fireNewLogFileLine(line + "\n");
						this.count++;
						log.info("{@line}=" + line + "\t{@count}=" + this.count);
					}
					// log.info("{@count}=" + this.count + "\t {@time}="
					// + System.currentTimeMillis());
					filePointer = file.getFilePointer();
				}
				TimeUnit.MILLISECONDS.sleep(this.sampleInterval);
				/*
				 * int curtime = (int)(System.currentTimeMillis()/1000); if
				 * (curtime > this.lifetime){ log.info("Tail -f ended:" +
				 * this.logfile + " count=" + this.count); break; }
				 */

				long lastModify = this.logfile.lastModified();
				long curtime = System.currentTimeMillis();
				long modifyInterval = 3600000L;
				if (lastModify > 0 && curtime - lastModify >= modifyInterval) {
					log.info("Tail -f ended:" + this.logfile + " count="
							+ this.count + ", with current time=" + curtime
							+ ", file modify time=" + lastModify);
					break;
				}
			}
			file.close();
			this.close();
		} catch (IOException e) {
			log.error(e.getMessage());
		} catch (InterruptedException e) {
			log.error(e.getMessage());
		}

	}
}
