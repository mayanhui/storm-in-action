package chapter08.storm.bolt;

import java.io.Serializable;

import chapter08.storm.spout.TupleInfo;

public class ThresholdInfo extends TupleInfo implements Serializable {

	private static final long serialVersionUID = 3337988877646687347L;

	private String action;
	private String rule;
	private static Integer thresholdValue;
	private static int thresholdColNumber;
	private static Integer timeWindow;
	private int frequencyOfOccurence;

	public ThresholdInfo() {

	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getRule() {
		return rule;
	}

	public void setRule(String rule) {
		this.rule = rule;
	}

	public static Integer getThresholdValue() {
		return thresholdValue;
	}

	public static void setThresholdValue(Integer thresholdValue) {
		ThresholdInfo.thresholdValue = thresholdValue;
	}

	public static int getThresholdColNumber() {
		return thresholdColNumber;
	}

	public static void setThresholdColNumber(int thresholdColNumber) {
		ThresholdInfo.thresholdColNumber = thresholdColNumber;
	}

	public static Integer getTimeWindow() {
		return timeWindow;
	}

	public static void setTimeWindow(Integer timeWindow) {
		ThresholdInfo.timeWindow = timeWindow;
	}

	public int getFrequencyOfOccurence() {
		return frequencyOfOccurence;
	}

	public void setFrequencyOfOccurence(int frequencyOfOccurence) {
		this.frequencyOfOccurence = frequencyOfOccurence;
	}

}
