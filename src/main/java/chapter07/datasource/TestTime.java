package chapter07.datasource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class TestTime {
	public static void main(String []args) throws IOException{
		BufferedReader br=null;
		BufferedWriter bw=null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		File file = new File("h:\\新建文件夹\\new");
		  File[] subFile = file.listFiles();
		  for (int i = 0; i < subFile.length; i++) {
		    String filename = subFile[i].getName();
		    try {
				br=new BufferedReader(new FileReader(new File("h:\\新建文件夹\\new\\"+filename)));
				bw=new BufferedWriter(new FileWriter(new File("h:\\test\\timetest"+i+".txt")));
				bw.write(filename);
				String str=br.readLine();
				while(str!=null){
					String []arr=str.split("\t",-1);
					long time=Long.parseLong(arr[0]);
					String data=sdf.format(new Date(time));
					bw.write(data);
					bw.newLine();
					str=br.readLine();
				}
				bw.flush();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				if(br!=null){
					br.close();
				}
				if(bw!=null){
					bw.close();
				}
			}
		   }
	}

}
