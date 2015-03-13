package chapter08.storm.spout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import chapter08.storm.bolt.SpeedProcessBolt;

public class SocketJava {

	private static final short POSINFO_PLATE_NUMBER = 0x0001;
	private static final short POSINFO_LONGITUDE = 0x0002;
	private static final short POSINFO_LATITUDE = 0x0003;
	private static final short POSINFO_REPORT_TIME = 0x0004;
	private static final short POSINFO_DEV_ID = 0x0005;
	private static final short POSINFO_SPEED = 0x0006;
	private static final short POSINFO_DIRECTION = 0x0007;
	private static final short POSINFO_LOCATION_STATUS = 0x0008;

	private static final short ALARMINFO_SIM_NUMBER = 0x0010;
	private static final short ALARMINFO_CAR_STATUS = 0x0011;
	private static final short ALARMINFO_CAR_COLOUR = 0x0012;

	public static Socket sock;
	static String GPSline = new String();
	static String out = new String();
	static int failedCount = 0;
	static int unknownCount = 0;

	static OutputStream output = null;
	static String encryptText = "siatxdata\r\ngps\r\n";

	static String ip = "192.168.170.10";

	public static void main(String[] args) throws Exception {

		try {
			if (sock == null)
				sock = new Socket(ip, 5557);
			output = sock.getOutputStream();
			output.write(encryptText.getBytes());
			output.flush();
			System.out.println("#---------连接成功，数据接收中.........\n");
			System.out.println("#    .为一条位置信息\n");
			System.out.println("#    #为一条警报信息\n");
			System.out.println("#    *为一条营运数据\n");

			int count = 0;
			int ch = 0;
			while (true) {

				byte[] b3 = new byte[3];
				if (sock != null) {
					try {
						sock.getInputStream().read(b3, 0, 3);
						ch = b3[0];
					} catch (Exception e) {
						System.out
								.println("connection reset, reconnecting ...");
						sock.close();
						Thread.sleep(1000);
						sock = new Socket(ip, 15025);
						output = sock.getOutputStream();
						output.write(encryptText.getBytes());
						output.flush();
					}

				} else {
					sock = new Socket(ip, 5557);
					output = sock.getOutputStream();
					output.write(encryptText.getBytes());
					output.flush();
					break;
				}

				int len = bytesToShort(b3, 1);
				if (len < 0)
					break;
				byte[] bytelen = new byte[len];
				sock.getInputStream().read(bytelen);
				if (bytelen == null) {
					System.out
							.println("read the second part from byte from socket failed ! ");
					break;
				}

				DissectOneMessage(ch, bytelen);
				System.out.println(count++ + ":\n");
			}

		} catch (UnknownHostException e) {
			sock.close();
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
			sock.close();
		}

		finally {
			try {
				if (sock != null)
					sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static String DissectOneMessage(int ch, byte[] msg) throws Exception {
		switch (ch) {
		case 0x02:
			System.out.print(".");
			String mm = DissectPositionInfo(msg, msg.length);
			return mm;
		case 0x03:
			System.out.print("#");
			break;
		case 0x04:
			System.out.print("*");
			break;
		default:
			System.out.println("\n	@@@		Unknow unit code, unit code=" + ch
					+ "		@@@\n");
			Thread.sleep(1000);
			unknownCount++;
			System.out.println(unknownCount);
			if (unknownCount >= 3) {
				sock.close();
				Thread.sleep(1000);
				System.out.println("Reconnecting ...");
				sock = new Socket("210.75.252.138", 5557);
				output = sock.getOutputStream();
				output.write(encryptText.getBytes());
				output.flush();
				unknownCount = 0;
			}

			break;
		}
		return null;
	}

	public static String DissectPositionInfo(byte[] msg, int len)
			throws Exception {
		short offset = 0;
		short unit_id = -1;
		short unit_len = 0;
		byte[] unit_value = null;

		String plate = null;
		short tempshort1 = 0, tempshort2 = 0, tempshort3 = 0, tempshort4 = 0, tempshort5 = 0, tempshort6 = 0;

		char tempchar = 0;

		while (offset <= len - 2) {
			unit_id = bytesToShort(msg, offset);

			offset = (short) (offset + 2);

			unit_len = (short) bytesToShort(msg, offset);
			// System.out.println("	Unit len=: "+unit_len);

			if (unit_len + offset <= len) {
				offset = (short) (offset + 2);

				unit_value = new byte[unit_len];
				for (int i = 0; i < unit_len; i++) {

					unit_value[i] = msg[offset + i];
				}
				offset = (short) (offset + unit_len);
			} else {
				break;
			}

			DecimalFormat df2 = (DecimalFormat) DecimalFormat.getInstance();

			switch (unit_id) {
			case POSINFO_PLATE_NUMBER:
				plate = new String(unit_value, "gbk");
				// System.out.println("	Plate number:"+plate+"\t");
				GPSline = plate + ",";
				plate = null;
				break;
			case POSINFO_LONGITUDE:
				long lon = 0;
				if (unit_len == 2) {
				} else if (unit_len == 4)
					lon = bytesToInt(unit_value);

				double dLon = lon / 1000000.0;
				df2.applyPattern("0.000000");
				GPSline = GPSline + df2.format(dLon) + ",";
				break;
			case POSINFO_LATITUDE:
				long lan = 0;
				if (unit_len == 2)
					lan = bytesToShort(unit_value);
				else if (unit_len == 4)
					lan = bytesToInt(unit_value);
				else if (unit_len == 8)
					lan = bytesToLong(unit_value);

				double dLan = lan / 1000000.0;
				df2.applyPattern("0.000000");
				GPSline = GPSline + df2.format(dLan) + ",";
				break;
			case POSINFO_REPORT_TIME:
				df2.applyPattern("00");
				tempshort1 = (short) bytesToShort(unit_value);
				tempshort2 = (short) unit_value[2];
				tempshort3 = (short) unit_value[3];
				tempshort4 = (short) unit_value[4];
				tempshort5 = (short) unit_value[5];
				tempshort6 = (short) unit_value[6];
				GPSline = GPSline + tempshort1 + "-" + df2.format(tempshort2)
						+ "-" + df2.format(tempshort3) + " "
						+ df2.format(tempshort4) + ":" + df2.format(tempshort5)
						+ ":" + df2.format(tempshort6) + ",";
				break;
			case POSINFO_DEV_ID:
				long sim = 0;
				if (unit_len == 2)
					sim = bytesToShort(unit_value);
				else if (unit_len == 4)
					sim = bytesToInt(unit_value);
				else if (unit_len == 8)
					sim = bytesToLong(unit_value);

				GPSline = GPSline + sim + ",";
				break;
			case POSINFO_SPEED:

				tempshort1 = (short) bytesToShort(unit_value);
				GPSline = GPSline + df2.format(tempshort1) + ",";
				break;
			case POSINFO_DIRECTION:
				if (unit_len >= 2) {
					tempshort1 = (short) bytesToShort(unit_value);
					GPSline = GPSline + (short) (tempshort1 / 100) + ",";
				}
				break;
			case POSINFO_LOCATION_STATUS:
				tempchar = (char) unit_value[0];
				tempshort1 = (short) tempchar;
				break;
			case ALARMINFO_SIM_NUMBER:
				plate = new String(unit_value, "GBK");
				plate = null;
				break;
			case ALARMINFO_CAR_STATUS:
				tempchar = (char) unit_value[0];
				tempshort1 = (short) tempchar;
				GPSline = GPSline + tempshort1 + ",";
				break;
			case ALARMINFO_CAR_COLOUR:
				plate = new String(unit_value, "GBK");
				GPSline = GPSline + plate + "\n";
				plate = null;

				SimpleDateFormat sdf2 = new SimpleDateFormat(
						"yyyy-MM-dd-HH-mm-ss");
				SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd-HH-");
				df2.applyPattern("00");

				Date nowDate = new Date();
				String nowTime = sdf2.format(nowDate);
				String cur_dir = System.getProperty("user.dir");
				String path = cur_dir + "/rawGPSData/" + sdf3.format(nowDate);
				SpeedProcessBolt.newFolder(path);

				int min = nowDate.getMinutes();
				int second = nowDate.getSeconds();
				if (min < 30) {
					min = 00;
					second = 00;
				} else if (min >= 30) {
					min = 30;
					second = 00;
				}
				path = cur_dir + "/rawGPSData/" + sdf3.format(nowDate) + "/"
						+ sdf4.format(nowDate) + df2.format(min) + "-"
						+ df2.format(second);

				return GPSline;

			default:
				System.out
						.println("\n	### 	Error: can't resort message info!   #### unit_id="
								+ unit_id + "\n");
				Thread.sleep(100);
				failedCount++;
				if (failedCount >= 3 && sock != null) {
					sock.close();
					Thread.sleep(1000);
					System.out.println("Reconnecting ...");
					sock = new Socket("210.75.252.138", 5557);
					output = sock.getOutputStream();
					output.write(encryptText.getBytes());
					output.flush();
					failedCount = 0;
				}
				break;

			}
		}
		return null;
	}

	public static short bytesToShort(byte[] b, int offset) {
		return (short) (b[offset + 1] & 0xff << 8 | (b[offset] & 0xff) << 0);
	}

	public static short bytesToShort(byte[] b) {
		return (short) ((b[1] & 0xff) << 8 | (b[0] & 0xff));// << 8);
	}

	public static long bytesToLong(byte[] array) {
		return ((((long) array[0] & 0xff) << 0)
				| (((long) array[1] & 0xff) << 8)
				| (((long) array[2] & 0xff) << 16)
				| (((long) array[3] & 0xff) << 24)
				| (((long) array[4] & 0xff) << 32)
				| (((long) array[5] & 0xff) << 40)
				| (((long) array[6] & 0xff) << 48) | (((long) array[7] & 0xff) << 56));
	}

	public static int bytesToInt(byte b[]) {
		return (b[3] & 0xff) << 24 | (b[2] & 0xff) << 16 | (b[1] & 0xff) << 8
				| (b[0] & 0xff) << 0;
	}

}
