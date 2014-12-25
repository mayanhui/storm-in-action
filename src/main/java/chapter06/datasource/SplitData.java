package chapter06.datasource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SplitData {

	@SuppressWarnings("rawtypes")
	public static List readfile(String filepath) throws FileNotFoundException,
			IOException {
		File file = new File(filepath);
		List<String> fileList = new ArrayList<String>();
		String[] filelist = file.list();
		for (int i = 0; i < filelist.length; i++) {
			File readfile = new File(filepath + "\\" + filelist[i]);
			// if (!readfile.isDirectory()) {
			// System.out.println("path=" + readfile.getPath());
			// System.out.println("absolutepath="
			// + readfile.getAbsolutePath());
			// System.out.println("name=" + readfile.getName());
			//
			// }
			String filePath = readfile.getAbsolutePath();
			fileList.add(filePath);
		}
		return fileList;
	}

	@SuppressWarnings({ "unused", "unchecked" })
	public static void main(String[] args) {
		BufferedReader br = null;
		BufferedWriter bw0 = null, bw1 = null, bw2 = null, bw3 = null, bw4 = null;
		String readLinestr = null;
		Random random = new Random();
		int fileNum = 0;
		BufferedWriter[] bwArr = new BufferedWriter[5];
		List<String> fileList = null;
		try {
			fileList = SplitData.readfile("E:\\GPSData");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (String fl : fileList) {

			File file = new File(fl);
			String fileName = file.getName();
			System.out.println(fileName);
			String[] fileNamep = fileName.split("\\.", -1);
			String filepath = "E:\\GPSData" + "\\N" + fileNamep[0];
			System.out.println(fileNamep[0]);
			File file0 = new File(filepath);

			try {

				br = new BufferedReader(new FileReader(fl));
				System.out.println(fl);
				file0.mkdir();
				bwArr[0] = new BufferedWriter(new FileWriter(filepath + "\\"
						+ fileNamep[0] + "-0.txt"));
				bwArr[1] = new BufferedWriter(new FileWriter(filepath + "\\"
						+ fileNamep[0] + "-1.txt"));
				bwArr[2] = new BufferedWriter(new FileWriter(filepath + "\\"
						+ fileNamep[0] + "-2.txt"));
				bwArr[3] = new BufferedWriter(new FileWriter(filepath + "\\"
						+ fileNamep[0] + "-3.txt"));
				bwArr[4] = new BufferedWriter(new FileWriter(filepath + "\\"
						+ fileNamep[0] + "-4.txt"));

				while ((readLinestr = br.readLine()) != null) {
					fileNum = random.nextInt(5);
					System.out.println(fileNum);
					System.out.println(readLinestr);
					bwArr[fileNum].write(readLinestr);
					bwArr[fileNum].newLine();
					bwArr[fileNum].flush();

				}

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}
	}
}
