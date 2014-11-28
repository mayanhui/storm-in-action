package chapter07.datasource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

//生成随机数据

public class RandomData {
	public static int noTel = 1;

	public static void main(String[] args) throws IOException {
		RandomData data = new RandomData();
		String str = null;
		int time = 104000000;
		// int tmp=107600000;
		String out = null;
		BufferedWriter bw = null;
		Random rd = new Random();
		int x = 13;
		int y = 0;
		for (int i = 1; i <= 168; i++) {
			int sum = rd.nextInt(5000000) + 3000000;
			int sum1 = rd.nextInt(234) + 127;
			out = "h:\\new\\" + x + "day" + y + "hours.txt";
			bw = new BufferedWriter(new FileWriter(new File(out)));
			for (int j = 0; j < sum; j++) {
				if (noTel % sum1 == 0) {
					str = data.RandomTime(time) + "\t" + "" + "\t"
							+ data.RandomMac() + "\t" + data.RandomIp() + "\t"
							+ data.RandomDom() + "\t" + data.RandomUpPack()
							+ "\t" + data.RandomDownPack() + "\t"
							+ data.RandomUpLoad() + "\t"
							+ data.RandomDownLoad() + "\t" + data.States();
				} else {

					str = data.RandomTime(time) + "\t" + data.RandomTele()
							+ "\t" + data.RandomMac() + "\t" + data.RandomIp()
							+ "\t" + data.RandomDom() + "\t"
							+ data.RandomUpPack() + "\t"
							+ data.RandomDownPack() + "\t"
							+ data.RandomUpLoad() + "\t"
							+ data.RandomDownLoad() + "\t" + data.States();
				}
				bw.write(str);
				bw.newLine();
				noTel++;
				// System.out.println(str);
			}
			bw.flush();
			time += 3600000;
			// time1+=3600000;
			y += 1;

			if (i % 24 == 0) {
				x += 1;
				y = 0;
			}

		}

	}

	//随机生成域名与对应网站类型
	public String RandomDom() {
		Random ran = new Random();
		String arr[] = { "baidu.com	搜索", "sina.com	门户", "alipay.com	支付",
				"suq.so.360.cn	信息安全", "Sl9.cnzz.com	站点统计", "tmall.com	购物",
				"taobao.com	购物", "jumei.com	购物", "baihe.com	相亲",
				"nuomi.com	团购", "tudou.com	视频", "mail.qq.com	邮箱",
				"jiayuan.com	相亲" };
		String dom = arr[ran.nextInt(arr.length)];
		return dom;
	}

	//随机生成网页对应状态码
	public String States() {
		Random ran = new Random();
		int n = ran.nextInt(10001);
		String sta = null;
		if (n == 0) {
			sta = "404";
		} else if (n == 1) {
			sta = "500";
		} else if (n == 2) {
			sta = "400";
		} else if (n == 3)
			sta = "401";
		else if (n == 4)
			sta = "402";
		else if (n == 5)
			sta = "504";
		else if (n == 6)
			sta = "414";
		else if (n == 7)
			sta = "424";
		else if (n == 8)
			sta = "301";
		else if (n == 9)
			sta = "307";
		else if (n == 10)
			sta = "405";
		else
			sta = "200";
		return sta;
	}

	//随机生成上行数据流量值
	
	public long RandomUpLoad() {
		Random ran = new Random();
		int up = ran.nextInt(8192);
		long re = up;
		return re;
	}

	//随机生成下行数据流量值
	
	public long RandomDownLoad() {
		Random ran = new Random();
		int down = ran.nextInt(8192);
		long re = down;
		return re;
	}

	//随机生成下行数据流量包
	
	public long RandomDownPack() {
		Random ran = new Random();
		int down = ran.nextInt(101);
		long re = down;
		return re;
	}

	//随机生成上行数据流量包
	public long RandomUpPack() {
		Random ran = new Random();
		int up = ran.nextInt(101);
		long re = up;
		return re;

	}

	//随机生成mac地址
	
	public String RandomMac() {
		Random ran = new Random();
		String arr[] = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A",
				"B", "C", "D", "E", "F" };
		String mac1 = arr[ran.nextInt(arr.length)];
		String mac2 = arr[ran.nextInt(arr.length)];
		String mac3 = arr[ran.nextInt(arr.length)];
		String mac4 = arr[ran.nextInt(arr.length)];
		String mac5 = arr[ran.nextInt(arr.length)];
		String mac6 = arr[ran.nextInt(arr.length)];
		String mac7 = arr[ran.nextInt(arr.length)];
		String mac8 = arr[ran.nextInt(arr.length)];
		String mac9 = arr[ran.nextInt(arr.length)];
		String mac10 = arr[ran.nextInt(arr.length)];
		String mac11 = arr[ran.nextInt(arr.length)];

		String mac12 = arr[ran.nextInt(arr.length)];
		String arr1[] = { "CMCC", "7Daysinn", "CMCC-EASY", "China-Uincom",
				"ChinaNet", "CMCC-EDU", "TD-SCDMA", "WCDMA", "CDMA2000", "ADSL" };
		String type = arr1[ran.nextInt(arr1.length)];

		return mac1 + mac2 + "-" + mac3 + mac4 + "-" + mac5 + mac6 + "-" + mac7
				+ mac8 + "-" + mac9 + mac10 + "-" + mac11 + mac12 + ":" + type;
	}
	
	//随机生成ipv4地址

	public String RandomIp() {
		Random ran = new Random();
		String str = null;
		while (true) {

			int ip1 = ran.nextInt(224) + 1;
			int ip2 = ran.nextInt(256);
			int ip3 = ran.nextInt(256);
			int ip4 = ran.nextInt(254) + 1;
			if (126 < ip1 & ip1 < 128 || ip1 == 10) {
				continue;
			} else if (ip1 == 172 & 15 < ip2 & ip2 < 32) {
				continue;
			} else if (ip1 == 192 & ip2 == 168) {
				continue;
			} else {
				str = ip1 + ":" + ip2 + ":" + ip3 + ":" + ip4;
				// System.out.println(str);
				return str;
				// System.out.println(str);
			}
		}
	}

	//随机生成时间数

	public String RandomTime(int m) {
		Random randT = new Random();
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// for (int i = 0; i < 20; i++) {
		int num = randT.nextInt(3600000) + m;
		String tmp = "1363" + num;
		// String date = sdf.format(new Date(time));
		// System.out.println(date);
		return tmp;
		// }
	}

	//随机生成手机（电话）号码
	
	public String RandomTele() {
		Random randP = new Random();
		// for(int i=0;i<100;i++){
		int type = randP.nextInt(5);
		if (type != 0) {
			String[] teles = { "13", "15", "18" };
			String tmptele = teles[randP.nextInt(teles.length)];
			int num = randP.nextInt(1000000000);
			String num1 = String.valueOf(num);
			String num2 = null;
			if (num1.length() < 9) {
				if (num1.length() < 8) {
					if (num1.length() < 7) {
						if (num1.length() < 6) {
							if (num1.length() < 5) {
								if (num1.length() < 4) {
									if (num1.length() < 3) {
										if (num1.length() < 2) {
											num2 = "00000000" + num1;
										} else
											num2 = "0000000" + num1;
									} else
										num2 = "000000" + num1;
								} else
									num2 = "00000" + num1;
							} else
								num2 = "0000" + num1;
						} else
							num2 = "000" + num1;
					} else
						num2 = "00" + num1;
				} else
					num2 = "0" + num1;

			} else
				num2 = num1;
			String tele = tmptele + num2;
			return tele;
			// System.out.println(tele);
		} else {
			int num3 = randP.nextInt(6) + 2;
			int num4 = randP.nextInt(10000000);
			String num5 = String.valueOf(num4);
			String num6 = null;
			if (num5.length() < 7) {
				if (num5.length() < 6) {
					if (num5.length() < 5) {
						if (num5.length() < 4) {
							if (num5.length() < 3) {
								if (num5.length() < 2) {
									num6 = "000000" + num5;
								} else
									num6 = "00000" + num5;
							} else
								num6 = "0000" + num5;
						} else
							num6 = "000" + num5;
					} else
						num6 = "00" + num5;
				} else
					num6 = "0" + num5;
			} else
				num6 = num5;
			String tele = num3 + num6;
			return tele;
			// System.out.println(tele);
			// }

		}
	}
}
