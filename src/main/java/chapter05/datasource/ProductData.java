package chapter05.datasource;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ProductData {

	public static long userTime;
	public static String userAccount;
	public static String[] userIP1 = { "223", "175", "202", "110", "221",
			"103", "118", "125" };
	public static String[] userIP2 = { "220", "184", "100", "166", "207", "22",
			"213", "72" };
	public static String[] userIP3 = { "0", "128", "255", "191", "135", "100",
			"63", "103", "144", "159", "136", "143" };
	public static String[] userIP4 = { "0", "255" };
	public static int skypeid;
	public static String innerIP;
	public static String privateValue;
	public static String[] devName1 = { "MacBook Pro mac", "iphone 5s",
			"Thinkpad" };
	public static String[] osName1 = { "os x 10.9", "ios", "win 8" };
	public static String str;
	public static String data;
	public static int skypeid1;
	public static String natIPTest;

	/**
	 * gen MD5 for string.
	 */
	public String md5s(String plainText) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(plainText.getBytes());
			byte b[] = md.digest();
			int i;
			StringBuffer buf = new StringBuffer("");
			for (int offset = 0; offset < b.length; offset++) {
				i = b[offset];
				if (i < 0)
					i += 256;
				if (i < 16)
					buf.append("0");
				buf.append(Integer.toHexString(i));
			}
			str = buf.toString();
			// System.out.println(buf.toString());// 32 bit encryption
			// System.out.println("result: " + buf.toString().substring(8,
			// 24));// 16 bit encryption
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return str;
	}

	/**
	 * create random array.
	 */
	public String getDev() {
		int index = (int) (Math.random() * devName1.length);
		return devName1[index];
	}

	public String getOs() {
		int index = (int) (Math.random() * osName1.length);
		return osName1[index];
	}

	public String getIP1() {
		int index = (int) (Math.random() * userIP1.length);
		return userIP1[index];
	}

	public String getIP2() {
		int index = (int) (Math.random() * userIP2.length);
		return userIP2[index];
	}

	public String getIP3() {
		int index = (int) (Math.random() * userIP3.length);
		return userIP3[index];
	}

	public String getIP4() {
		int index = (int) (Math.random() * userIP4.length);
		return userIP4[index];
	}

	public int getSkypeID() {
		skypeid1 = new Random().nextInt(999999999);
		return skypeid1;
	}

	public String getInnerIP() {
		java.util.Random rd = new java.util.Random();
		return rd.nextInt(255) + "." + rd.nextInt(255);
	}

	/**
	 * create data
	 */
	public static List<String> creatData() {
		ProductData pd = new ProductData();
		Random rd = new Random((long) (Math.random() * 75));
		List<String> dd = new ArrayList<String>();
		userTime = System.currentTimeMillis();
		innerIP = "192" + "." + "168" + "." + pd.getInnerIP();
		String userIP = pd.getIP1() + "." + pd.getIP2() + "." + pd.getIP3()
				+ "." + pd.getIP4();
		String devName = pd.getDev();
		String osName = pd.getOs();
		userAccount = "9" + rd.nextInt(9999999);
		if (userAccount.length() < 12) {
			String userModel = "1234567890";
			int a = 11 - userAccount.length();
			String userResult = userModel
					.substring((int) Math.random() * 10, a);
			userAccount = userAccount + userResult;
		}
		int times = rd.nextInt(25) + 1;
		skypeid = pd.getSkypeID();
		for (int i = 0; i < times; i++) {
			int s = rd.nextInt(3);
			if (s == 2) {
				skypeid = pd.getSkypeID();
				innerIP = "192" + "." + "168" + "." + pd.getInnerIP();
				privateValue = pd.md5s(userAccount);
			}
			String data = userTime + "\t" + userAccount + "\t" + userIP + "\t"
					+ skypeid + "\t" + innerIP + "\t" + privateValue + "\t" + devName
					+ "\t" + osName + "\n";
			dd.add(data);
		}
		return dd;
	}

	/**
	 * send socket-UDP
	 */

	public void udpSendSocket() {
		DatagramSocket ds;
		try {
			ds = new DatagramSocket();
			List<String> dataflumn = ProductData.creatData();
			// System.out.println(dataflumn);
			for (String list : dataflumn) {
				;
				byte[] by = list.getBytes();
				DatagramPacket dp = new DatagramPacket(by, by.length,
						InetAddress.getByName("192.168.2.53"), 10006);
				ds.send(dp);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * send socket-TCP
	 */
	public void tcpSendSocket() {
		Socket s1 = null;
		try {
			List<String> dataflumn = ProductData.creatData();
			// System.out.println(dataflumn);
			for (String list : dataflumn) {
				s1 = new Socket("192.168.2.101", 10034);
				BufferedOutputStream bosout = new BufferedOutputStream(
						s1.getOutputStream());
				bosout.write(list.getBytes());
				bosout.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != s1) {
				try {
					s1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * write data to local
	 */

	public static void sendLocal(String result) {
		FileOutputStream out = null;
		System.out.println();
		try {
			out = new FileOutputStream(
					"/opt/modules/workspace/storm-in-action/IODataTest1.txt",
					true);
			out.write(result.getBytes());
			out.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws UnknownHostException {
		Thread thread = new Thread(new SleepRunner());
		thread.start();

	}
}

/**
 * set rate of transform
 */
class SleepRunner implements Runnable {

	private ProductData pd = new ProductData();

	@SuppressWarnings("static-access")
	@Override
	public void run() {
		while (true) {
			try {
				pd.tcpSendSocket();
				Thread.currentThread().sleep((long) (Math.random() * 1000));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
