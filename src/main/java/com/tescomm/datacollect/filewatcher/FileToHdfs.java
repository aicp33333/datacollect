package com.tescomm.datacollect.filewatcher;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.tescomm.datacollect.utils.OozieRunner;

/**
 * 上传文件到HDFS
 * @author liubing
 *
 */
public class FileToHdfs {
	// 文本文件上传到hdfs。
	public static Logger logger = Logger.getLogger(FileToHdfs.class);

	/**
	 * put文件
	 * @param localsrc
	 * @param dirhdfs
	 * @return
	 * @throws Exception
	 */
	public static boolean localfileToHdfs(String localsrc, String dirhdfs) throws Exception {
		InputStream in = null;
		FileSystem fs = null;
		OutputStream out = null;
		try {
			
			in = new BufferedInputStream(new FileInputStream(localsrc));
			Configuration conf = new Configuration();
			fs = FileSystem.get(URI.create(dirhdfs), conf);
			String filename = new File(localsrc).getName();
			System.out.println("localsrc: "+localsrc+"   ----------    dirhdfs: "+dirhdfs);
//			out = fs.create(new Path(dirhdfs));
//			IOUtils.copyBytes(in, out, 4096, true);
			fs.copyFromLocalFile(new Path(localsrc), new Path(dirhdfs+"/"+filename));
			
			OozieRunner.jobRun("volte_mk", dirhdfs+"/"+filename, "", "20", 3);
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (out != null) {
				out.close();
			}
			if (fs != null) {
				fs.close();
			}
			if (in != null) {
				in.close();
			}
			logger.info("文件：" + localsrc + "上传完成！！");
		}
	}

	public static void main(String[] args) throws Exception {
		String loca1 = "/home/rp/data/lte.xlsx";

		String filename1 = "hdfs://10.95.3.138:8020/user/tescomm/rp";
 		boolean result1 = localfileToHdfs(loca1, filename1);
		// boolean result2=localfileToHdfs(loca2,filename2);
		System.out.println(result1 + "---------------");
	}
}
