package com.tescomm.datacollect.filewatcher;

import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import com.tescomm.datacollect.utils.CustomFileUtils;
import com.tescomm.datacollect.utils.DateUtils;

public class QueueFileToHdfs implements Runnable {
	public static Logger logger = Logger.getLogger(QueueFileToHdfs.class);

	private static Pattern pattern = Pattern.compile("\\,");
	String[] path;
	String src;
	String filename;
	//String dest;
	String suffix;
	String check_suffix;
	String back_path;
	String src_filename;
	String dest_path;
	
	Map<String, String> map = null;
	Map<String, String> part_map = null;
	// String
	int number = 0;

	public QueueFileToHdfs() {
	}

	/**
	 * 构造函数
	 * @param suffix  文件后缀
	 * @param check_suffix  效验文件文件后缀
	 * @param back_path  备份文件路径
	 * @param dest_path  文件上传目的路径
	 * @param map   源文件目录文件夹与上传目路径文件夹对应关系
	 */
	public QueueFileToHdfs(String suffix, String check_suffix, String back_path,  String dest_path, Map<String, String> map, Map<String , String> part_map) {
		this.suffix = suffix;
		this.check_suffix = check_suffix;
		this.back_path = back_path;
		this.dest_path = dest_path;
		this.map = map;
		this.part_map = part_map;
	}

	@Override
	public void run() {
		while (true) {
			// 判断队列内是否存在待处理任务
			if (WatcherServer.queue.isEmpty()) {
				// 没有相应继续等待
				try {
					Thread.sleep(300);
					continue;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// 得到首行的任务并移除
			path = pattern.split(WatcherServer.queue.poll());
			src = path[0];
			src_filename = path[1];
//			logger.info("queue中发现消息: "+src+" -- " +dest_path+"======="+src_filename);
			// 判断文件结尾是否正确
			if (src.endsWith(suffix)) {
				// 得到文件名
				filename = new File(src).getName();
				
//				String date = filename.substring(filename.indexOf("_")+1, filename.indexOf("_")+9);
				try {
					// 上传文件到HDFS
					//partitionString方法是获取时间分区，需修改成可配置的
					FileToHdfs.localfileToHdfs(src, dest_path+"/"+map.get(src_filename));
					logger.info(src+",yuanmudi:"+ dest_path+"/"+map.get(src_filename));
					partitionData(Integer.valueOf(part_map.get(map.get(src_filename))));
					//CustomFileUtils.moveFile(src.toString().substring(0, src.toString().indexOf(".")) + check_suffix, back_path + "/" +src_filename+"/");
					//删除。ok文件
					CustomFileUtils.deleteFile(src.toString().substring(0, src.toString().indexOf(".")) + check_suffix);
					CustomFileUtils.moveFile(src, back_path + "/" +src_filename+"/");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 通过文件名获取分区
	 * @param filename
	 * @return
	 */
	private static String partitionString(String filename){
		String date=filename.substring(filename.indexOf("_")+1, filename.indexOf("_")+9);
		String hour=filename.substring(filename.indexOf("_")+9, filename.indexOf("_")+11);
		String result=date+"/1"+hour+"0000/";
//		System.out.println(result);
		return result;
	}
	
	/**
	 * 根据配置文件获取分区-按时间粒度
	 * @param s
	 * @return
	 */
	private static String partitionData(int s){
		if(s == 24){
			DateUtils.systemDate("yyyyMMdd");
		}else if(s == 60){
			DateUtils.systemDate("yyyyMMddhh");
		}else if(s == 5){
			
		}
		return " ";
	}
}
