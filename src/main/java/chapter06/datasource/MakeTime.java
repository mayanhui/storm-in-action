package chapter06.datasource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/*class parseTime {
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
}*/
public class MakeTime {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BufferedReader br = null;
		BufferedWriter bw = null;
		int count = 0, fileNum= 0, carSign = 0, speed = 0, bearing = 0, occupied = 0;
		double longitude = 0, lantitude = 0;
		Date date_time = new Date();
		DateFormat format = new SimpleDateFormat("2014-02-08 17:59:58");
		String time = format.format(date_time);
		parseTime pt=new parseTime();
		String newTime=pt.add(time, 30);
		System.out.println(newTime);
		//GregorianCalendar gc=new GregorianCalendar();
		
		String record = null;
		// br=new BufferedReader(new FileReader(filepath));
		Random random = new Random();
		Set<String> carNum=new HashSet<String>();
//		try {
//			br=new BufferedReader(new FileReader("E:\\GPSData\\1.txt"));
//			
//			bw = new BufferedWriter(new FileWriter("E:\\GPSData\\2.txt"));
//			
//
//			
//				longitude = random.nextDouble() + 117;
//				lantitude = random.nextDouble() + 40;
//				speed = random.nextInt(71);
//				bearing = random.nextInt(361);
//				occupied = random.nextInt(2);
//				record = "热" + ""+ "," + longitude
//						+ "," + lantitude + "," + time + "," + speed + ","
//						+ bearing + "," + occupied;
//				bw.write(record);
//				bw.newLine();
//				bw.flush();
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
	}

}
