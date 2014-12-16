package chapter06.kafka.producer.tail;

public interface LogFileTailerListener {
    /**
     * A new line has been added to the tailed log file
     * 
     * @param line The new line that has been added to the tailed log file
     */
    public void newLogFileLine(String line);
    public void close();
}
