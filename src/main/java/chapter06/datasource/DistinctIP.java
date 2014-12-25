package chapter06.datasource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class DistinctIP {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		String str = null;
		Set<String> lset = new HashSet<String>();
		try {
			br = new BufferedReader(new FileReader("IpData-ip.txt"));
			bw = new BufferedWriter(new FileWriter("IpData-ipnew.txt"));
			while ((str = br.readLine()) != null) {

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Random r = new Random();
		int d = r.nextInt(16);
	}

}
