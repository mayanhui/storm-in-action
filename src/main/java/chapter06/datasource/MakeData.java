package chapter06.datasource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

class parseTime {
	public String add(String time, int s) {
		String[] timeArr = time.split(" ");
		String[] timeArr2 = timeArr[1].split(":");
		int hour = Integer.parseInt(timeArr2[0]);
		int minute = Integer.parseInt(timeArr2[1]);
		int second = Integer.parseInt(timeArr2[2]);
		second += s;
		if (second >= 60) {
			minute++;
			second = second - 60;
			if (minute >= 60) {
				hour++;
				minute = minute - 60;
			}
		}
		return timeArr[0] + " " + hour + ":" + minute + ":" + second;

	}
}

public class MakeData {


	public static void main(String[] args) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		int count = 0, index = 0, carSign = 0, speed = 0, bearing = 0, occupied = 0, fileNum = 0, extSecond = 0;
		double longitude = 0, lantitude = 0, extLo = 0, extLa = 0;
		Date date_time = new Date();
		DateFormat format = new SimpleDateFormat("2014-02-08 17:29:58");
		String time = format.format(date_time);
		parseTime pt = new parseTime();
		String carNumstr = "", readRecordLine = null;
		char[] str = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
		String record = null;
		Random random = new Random();
		Set<String> carNum = new HashSet<String>();
		for (; fileNum < 2; fileNum++) {
			try {
				bw = new BufferedWriter(new FileWriter("GPSData-"
						+ (fileNum + 1) + ".txt"));
				File file = new File("E:\\GPSData\\" + fileNum + ".txt");
				if (file.exists()) {
					br = new BufferedReader(new FileReader("GPSData-" + fileNum
							+ ".txt"));
					readRecordLine = br.readLine();
					while (readRecordLine != null) {
						String[] recordLine = readRecordLine.split(",");
						extLo = random.nextDouble() / 100;
						extLa = random.nextDouble() / 100;
						double newlongitude = Double.parseDouble(recordLine[1])
								- 0.005 + extLo;
						double newlantitude = Double.parseDouble(recordLine[2])
								- 0.005 + extLa;
						String newTime = pt.add(recordLine[3], 30);
						speed = random.nextInt(71);
						bearing = random.nextInt(361);
						if (speed == 0) {
							occupied = 1;
						} else {
							occupied = 0;
						}

						record = recordLine[0] + ","
								+ String.format("%.6f", newlongitude) + ","
								+ String.format("%.6f", newlantitude) + ","
								+ newTime + "," + speed + "," + bearing + ","
								+ occupied;
						readRecordLine = br.readLine();
						bw.write(record);
						bw.newLine();
						bw.flush();
					}

				} else if (readRecordLine == null) {
					for (; carNum.size() < 300000;) {
						carSign = random.nextInt(90) % 26 + 65;
						carNumstr = (char) carSign + "";
						while (count < 5) {
							index = random.nextInt(10);
							carNumstr += str[index];
							count++;
						}
						carNum.add(carNumstr);
						carNumstr = "";
						count = 0;
					}
					for (String s : carNum) {
						longitude = random.nextDouble() + 117;
						lantitude = random.nextDouble() + 40;
						extSecond = random.nextInt(3);
						time = pt.add(time, extSecond);
						speed = random.nextInt(71);
						bearing = random.nextInt(361);
						if (speed == 0) {
							occupied = 1;
						} else {
							occupied = 0;
						}

						record = "热" + s + ","
								+ String.format("%.6f", longitude) + ","
								+ String.format("%.6f", lantitude) + "," + time
								+ "," + speed + "," + bearing + "," + occupied;
						bw.write(record);
						bw.newLine();
						bw.flush();
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

			// System.out.println(record);

		}
	}

}
