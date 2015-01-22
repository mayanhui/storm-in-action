package chapter07.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDao extends Thread {
	public Configuration config;
	public static HTable table;
	public static HBaseAdmin admin;

	public HBaseDao() {
		this.config = HBaseConfiguration.create();
		this.config.set("hbase.master", "192.168.32.128:60000");
		this.config.set("hbase.zookeeper.property.clientPort", "2181");
		this.config.set("hbase.zookeeper.quorum", "192.168.32.128");
		try {
			table = new HTable(this.config, Bytes.toBytes("weibo"));
			admin = new HBaseAdmin(this.config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void Read() throws IOException {
		List list = new ArrayList();
		Scan scan = new Scan();
		scan.setBatch(0);
		scan.setCaching(10000);
		scan.setMaxVersions();
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("total"));
		ResultScanner rsScanner = table.getScanner(scan);
		for (Result rs : rsScanner) {
			String date = Bytes.toString(rs.getRow());
			String total = Bytes.toString(rs.getValue(Bytes.toBytes("cf1"),
					Bytes.toBytes("total")));
			list.add(date + "\t" + total);
		}

		for (int i = 0; i < 7; i++)
			System.out.println((String) list.get(i) + "\t"
					+ (String) list.get(i + 7));
	}

	@SuppressWarnings({ "resource", "rawtypes", "unchecked" })
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "192.168.32.128:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "192.168.32.128");
		HTable table = new HTable(config, Bytes.toBytes("weibo"));
		List list = new ArrayList();
		Scan scan = new Scan();
		scan.setBatch(0);
		scan.setCaching(10000);
		scan.setMaxVersions();
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("total"));
		ResultScanner rsScanner = table.getScanner(scan);
		for (Result rs : rsScanner) {
			String date = Bytes.toString(rs.getRow());
			String total = Bytes.toString(rs.getValue(Bytes.toBytes("cf1"),
					Bytes.toBytes("total")));
			list.add(date + "\t" + total);
		}

		for (int i = 0; i < 7; i++)
			System.out.println((String) list.get(i) + "\t"
					+ (String) list.get(i + 7));
	}
}