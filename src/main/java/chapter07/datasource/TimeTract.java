package chapter07.datasource;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeTract {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String date7 = sdf.format(new Date(1363104000000L));
		System.out.println(date7);
		String date8 = sdf.format(new Date(1363708799000L));
		System.out.println(date8);
		String date9 = sdf.format(new Date(1363106528285L));
		System.out.println(date9);
	}
}
