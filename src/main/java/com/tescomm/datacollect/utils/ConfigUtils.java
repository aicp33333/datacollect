package com.tescomm.datacollect.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ConfigUtils {
	
	private static Logger logger = Logger.getLogger(ConfigUtils.class);
	
	/**
	 * 读取properties配置文件信息
	 * @param filePath  文件路径
	 * @return
	 */
	public static Properties getConfig(String filePath) {
		InputStream inputStream = null;
		Properties prop = new Properties();
		try {
			inputStream = ConfigUtils.class.getResourceAsStream(filePath);
			prop.load(inputStream);
		} catch (Exception e) {
			logger.info("init properties error: " + filePath);
			e.printStackTrace();
		}finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					logger.info("can't close the inputstream!");
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
	
	public static void main(String[] args) {
		Properties prop = ConfigUtils.getConfig("/config/redis_meta_data.properties");
		System.out.println(prop.getProperty("REDIS.HOST"));
	}
}
