package com.tescomm.datacollect.utils;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
public class CustomFileUtils {
	private static Logger logger = Logger.getLogger(CustomFileUtils.class);
	/**
	 * 移动文件或者目录,移动前后文件完全一样,如果目标文件夹不存在则创建。
	 * 
	 * @param resFilePath 源文件路径
	 * @param distFolder 目标文件夹
	 * @IOException 当操作发生异常时抛出
	 */
	public synchronized static void moveFile(String resFilePath, String distFolder) throws IOException {
		File resFile = new File(resFilePath);
		File distFile = new File(distFolder+"/"+resFile.getName());
		logger.info("将源文件:"+resFilePath+", 移动到目的路径dist：" + distFile);
		// 判断是文件还是目录
		if (resFile.isDirectory()) {
			if(distFile.isDirectory()){
				distFile = new File(distFolder+"_back");
			}
			// 目录
			FileUtils.moveDirectoryToDirectory(resFile, distFile, true);
		} else if (resFile.isFile()) {
			// 文件
			// 判断文件是否已经存在
			if(distFile.isFile()){
				// 如果存在添加别名, 防止文件名重复
				distFile = new File(distFolder+"/"+resFile.getName()+"_back_"+DateUtils.date2String(new Date(), "yyyyMMdd-hhmmss"));
			}
			FileUtils.moveFile(resFile, distFile);
		}
	}
	/**
	 * 删除一个文件或者目录
	 * 
	 * @param targetPath
	 *            文件或者目录路径
	 * @IOException 当操作发生异常时抛出
	 */
	public synchronized static void deleteFile(String targetPath) throws IOException {
		File targetFile = new File(targetPath);
		if (targetFile.isDirectory()) {
			FileUtils.deleteDirectory(targetFile);
		} else if (targetFile.isFile()) {
			targetFile.delete();
		}
	}
	
	public static void main(String[] args){
		try {
			moveFile("/opt/1.ok", "/opt/1");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

}
