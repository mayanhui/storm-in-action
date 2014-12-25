package chapter06.datasource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeFile {

	@SuppressWarnings("rawtypes")
	public static List readfile(String filepath) throws FileNotFoundException,
			IOException {
		File file = new File(filepath);
		List<String> fileList = new ArrayList<String>();
		String[] filelist = file.list();
		for (int i = 0; i < filelist.length; i++) {
			File readfile = new File(filepath + "\\" + filelist[i]);
			if (readfile.isDirectory()) {
				// System.out.println("path=" + readfile.getPath());
				// System.out.println("absolutepath="
				// + readfile.getAbsolutePath());
				// System.out.println("name=" + readfile.getName());
				String deractoryName = readfile.getName();
				fileList.add(deractoryName);
			}

		}
		return fileList;
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		FileInputStream input = null;
		FileOutputStream output = null;

		List<String> deractoryList = new ArrayList<String>();
		try {
			deractoryList = DistributeFile.readfile("E:\\GPSData\\");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (String list : deractoryList) {
			String[] fileNamep = list.split("\\.", -1);
			for (int fileNum = 0; fileNum < 5; fileNum++) {
				try {

					input = new FileInputStream("E:\\GPSData\\" + list + "\\"
							+ fileNamep[0].substring(1, 2) + "-" + fileNum
							+ ".txt");
					switch (fileNum) {
					case 0:
						output = new FileOutputStream(
								"E:\\DistributeData\\MachineOne\\"
										+ fileNamep[0].substring(1, 2) + "-"
										+ fileNum + ".txt");
						break;
					case 1:
						output = new FileOutputStream(
								"E:\\DistributeData\\MachineTwo\\"
										+ fileNamep[0].substring(1, 2) + "-"
										+ fileNum + ".txt");
						break;
					case 2:
						output = new FileOutputStream(
								"E:\\DistributeData\\MachineThree\\"
										+ fileNamep[0].substring(1, 2) + "-"
										+ fileNum + ".txt");
						break;
					case 3:
						output = new FileOutputStream(
								"E:\\DistributeData\\MachineFour\\"
										+ fileNamep[0].substring(1, 2) + "-"
										+ fileNum + ".txt");
						break;
					case 4:
						output = new FileOutputStream(
								"E:\\DistributeData\\MachineFive\\"
										+ fileNamep[0].substring(1, 2) + "-"
										+ fileNum + ".txt");
						break;
					}

					int i = input.read();
					while (i != -1) {
						output.write(i);
						i = input.read();
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
