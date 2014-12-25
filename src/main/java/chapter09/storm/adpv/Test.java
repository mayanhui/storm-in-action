package chapter09.storm.adpv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	private static Configuration conf = null;
	/**
	 * 初始化配置
	 */
	static {
		conf = HBaseConfiguration.create();
	}

	// 3.2 创建表

	/**
	 * 创建表操作
	 * 
	 * @throws IOException
	 */
	public void createTable(String tablename, String[] cfs) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("表已经存在！");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("表创建成功！");
		}
	}

	// 3.3 删除表

	/**
	 * 删除表操作
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public void deleteTable(String tablename) throws IOException {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			System.out.println("表删除成功！");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	// 3.4 插入一行记录

	/**
	 * 插入一行记录
	 * 
	 * @param tablename
	 * @param cfs
	 */
	public void writeRow(String tablename, String[] cfs) {
		try {
			HTable table = new HTable(conf, tablename);
			Put put = new Put(Bytes.toBytes("rows1"));
			for (int j = 0; j < cfs.length; j++) {
				put.add(Bytes.toBytes(cfs[j]),
						Bytes.toBytes(String.valueOf(1)),
						Bytes.toBytes("value_1"));
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 3.5 删除一行记录

	/**
	 * 删除一行记录
	 * 
	 * @param tablename
	 * @param rowkey
	 * @throws IOException
	 */
	public void deleteRow(String tablename, String rowkey) throws IOException {
		HTable table = new HTable(conf, tablename);
		List list = new ArrayList();
		Delete d1 = new Delete(rowkey.getBytes());
		list.add(d1);
		table.delete(list);
		System.out.println("删除行成功！");
	}

	// 3.6 查找一行记录

	/**
	 * 查找一行记录
	 * 
	 * @param tablename
	 * @param rowkey
	 */
	public static void selectRow(String tablename, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tablename);
		Get g = new Get(rowKey.getBytes());
		Result rs = table.get(g);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + "");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + "");
			System.out.print(kv.getTimestamp() + "");
			System.out.println(new String(kv.getValue()));
		}
	}

	// 3.7 查询表中所有行

	/**
	 * 查询表中所有行
	 * 
	 * @param tablename
	 */
	public void scaner(String tablename) {
		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan();
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				KeyValue[] kv = r.raw();
				for (int i = 0; i < kv.length; i++) {
					System.out.print(new String(kv[i].getRow()) + "");
					System.out.print(new String(kv[i].getFamily()) + ":");
					System.out.print(new String(kv[i].getQualifier()) + "");
					System.out.print(kv[i].getTimestamp() + "");
					System.out.println(new String(kv[i].getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
