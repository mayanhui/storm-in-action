package chapter06.kafka.producer.tail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class DirectoryMoniter implements Runnable {
	static Logger log = Logger.getLogger(DirectoryMoniter.class);
	private Set<File> subDirectorys;
	private File baseDirectory;
	private Properties p;
	private int type; // 1:one logfile to one topic; 2:one logfile to multitopic
	private String topic;
	private Map<String, String> topicMapping;
	private Set<String> doingFiles;
	String[] filenamefilters;

	// private Set tailerSet;

	public DirectoryMoniter(Properties prop) throws IOException {
		log.info("DirectoryMoniter init");
		this.p = prop;
		this.baseDirectory = new File(this.p.getProperty("baseDirectory"));
		if (!this.baseDirectory.isDirectory()) {
			throw new RuntimeException("directory mast be a directory");
		}
		this.subDirectorys = new HashSet<File>();
		this.doingFiles = new HashSet<String>();
		// this.tailerSet = new HashSet();
		this.type = Integer.valueOf(this.p.getProperty("type"));
		this.filenamefilters = this.p.getProperty("filenamefilters").split(",");
		if (this.type == 1) {
			this.topic = this.p.getProperty("topic");
			log.info("DirectoryMoniter init:baseDirectory="
					+ this.baseDirectory + ";type=" + this.type + ";topic="
					+ this.topic);
		} else if (this.type == 2) {
			this.topicMapping = new HashMap<String, String>();
			for (String topicMap : this.p.getProperty("topicMapping")
					.split(",")) {
				String[] tmp = topicMap.split(":");
				this.topicMapping.put(tmp[0], tmp[1]); // <keyword,topic>
				log.info("DirectoryMoniter init:baseDirectory="
						+ this.baseDirectory + ";type=" + this.type + ";topic="
						+ tmp[1]);
			}
		} else {
			throw new RuntimeException("type error type=[" + this.type + "]");
		}
	}

	public DirectoryMoniter(String configFile) throws IOException {
		log.info("DirectoryMoniter init");
		FileInputStream ins = new FileInputStream(configFile);
		this.p = new Properties();
		p.load(ins);
		ins.close();
		this.baseDirectory = new File(this.p.getProperty("baseDirectory"));
		if (!this.baseDirectory.isDirectory()) {
			throw new RuntimeException("directory mast be a directory");
		}
		this.subDirectorys = new HashSet<File>();
		this.doingFiles = new HashSet<String>();
		// this.tailerSet = new HashSet();
		this.type = Integer.valueOf(this.p.getProperty("type"));
		this.filenamefilters = this.p.getProperty("filenamefilters").split(",");
		if (this.type == 1) {
			this.topic = this.p.getProperty("topic");
			log.info("DirectoryMoniter init:baseDirectory="
					+ this.baseDirectory + ";type=" + this.type + ";topic="
					+ this.topic);
		} else if (this.type == 2) {
			this.topicMapping = new HashMap<String, String>();
			for (String topicMap : this.p.getProperty("topicMapping")
					.split(",")) {
				String[] tmp = topicMap.split(":");
				this.topicMapping.put(tmp[0], tmp[1]); // <keyword,topic>
				log.info("DirectoryMoniter init:baseDirectory="
						+ this.baseDirectory + ";type=" + this.type + ";topic="
						+ tmp[1]);
			}
		} else {
			throw new RuntimeException("type error type=[" + this.type + "]");
		}
	}

	private boolean fileNameMatches(String filename) {
		return true;
		// for (String filter : this.filenamefilters){
		// if (filter.length() == 0){
		// return true;
		// }
		// if (filename.matches(filter)){
		// return true;
		// }
		// }
		// return false;
	}

	private void createTailer(String filename) {
		if (this.type == 1) {
			@SuppressWarnings("unused")
			TailProducer tailer = new TailProducer(filename,
					this.p.getProperty("zkconnect"), this.topic,
					Integer.valueOf(this.p.getProperty("runtime")),
					Long.valueOf(this.p.getProperty("interval")));
			// this.tailerSet.add(tailer);
		} else if (this.type == 2) {
			@SuppressWarnings("unused")
			MutliTailProducer mtailer = new MutliTailProducer(filename,
					this.p.getProperty("zkconnect"), this.topicMapping,
					Integer.valueOf(this.p.getProperty("runtime")),
					Long.valueOf(this.p.getProperty("interval")));
			// this.tailerSet.add(mtailer);
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		long modifyInterval = 600000L;
		Set<File> tmp = new HashSet<File>();
		Set<File> deleted = new HashSet<File>();
		while (true) {
			try {
//				TimeUnit.MILLISECONDS.sleep(5000);
				TimeUnit.MILLISECONDS.sleep(1000);
				for (File f : this.baseDirectory.listFiles()) {
					/*if (f.isDirectory()) {
						if (!this.subDirectorys.contains(f)) {
							this.subDirectorys.add(f);
							log.info("directory [" + f.getAbsolutePath()
									+ "] added to moniter");
						}
					} else */
						
						if (f.isFile()) {
						if (!this.doingFiles.contains(f.getAbsolutePath())) {
							long lastModify = f.lastModified();
							long curtime = System.currentTimeMillis();
							if (curtime - lastModify < modifyInterval) {
								log.info("create tailer!");
								this.createTailer(f.getAbsolutePath());
								this.doingFiles.add(f.getAbsolutePath());
								log.info("DirectoryMoniter:baseDirectory="
										+ this.baseDirectory + ";addLogTailer "
										+ f.getAbsolutePath());
								log.info("curtime[" + curtime
										+ "] - lastModified[" + lastModify
										+ "] = " + (curtime - lastModify));
							}
						}
					}
				}

				for (File f : this.subDirectorys) {
					if (!f.exists()) {
						deleted.add(f);
						continue;
					}
					for (File sf : f.listFiles()) {
						if (sf.isDirectory()) {
							if (!this.subDirectorys.contains(sf)) {
								// this.subDirectorys.add(sf);
								tmp.add(sf);
								log.info("DirectoryMoniter:baseDirectory="
										+ this.baseDirectory + ";addSubDir "
										+ sf.getAbsolutePath());
							}
						}
					}
				}
				this.subDirectorys.removeAll(deleted);
				this.subDirectorys.addAll(tmp);
				tmp.clear();
				deleted.clear();

				for (File f : this.subDirectorys) {
					for (File sf : f.listFiles()) {
						if (sf.isFile() && this.fileNameMatches(sf.getName())) {
							if (!this.doingFiles.contains(sf.getAbsolutePath())) {
								long lastModify = sf.lastModified();
								long curtime = System.currentTimeMillis();
								if (curtime - lastModify < modifyInterval) {
									this.createTailer(sf.getAbsolutePath());
									this.doingFiles.add(sf.getAbsolutePath());
									log.info("DirectoryMoniter:baseDirectory="
											+ this.baseDirectory
											+ ";addLogTailer "
											+ sf.getAbsolutePath());
									log.info("curtime[" + curtime
											+ "] - lastModified[" + lastModify
											+ "] = " + (curtime - lastModify));
								}
							}
						}
					}
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
				log.error(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				log.error(e.getMessage());
				log.error(e.toString());
			}
		}
	}
}
