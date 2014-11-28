package chapter07.datasource;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class TimeTract {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
//		 Date d = new Date();
//		  SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM-dd  kk:mm:ss ");
//		  System.out.println(sdf1.format(d));
		// TODO Auto-generated method stub
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	//	 sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
		String date7 = sdf.format(new Date(1363104000000L));
		System.out.println(date7);
		String date8 = sdf.format(new Date(1363708799000L));
		System.out.println(date8);
//		String date8 = sdf.format(new Date(1363708800000L));
//		System.out.println(date8);
		String date9 = sdf.format(new Date(1363106528285L));
		System.out.println(date9);
	}
}
