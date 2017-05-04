/**
 * 
 */
package com.tescomm.datacollect.utils;
 
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
 

/**
 * @author admin
 *
 */
 


 

public class OozieRunner implements Constant {

	// split_size 默认值
	private static final String SPLIT_SIZE = "64";
	// redtasks  默认值
	private static final String REDTASKS = "20";
	// 原始数据保留天数(cache目录)
	private static final int HIS_DATA = 3;

	/**
	 * args[] jobname inputpath split_size(m) redtasks his_data
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == 5) {
			jobRun(args[0], args[1], args[2], args[3], Integer.valueOf(args[4]));
		} else if (args.length == 2) {
			jobRun(args[0], args[1], SPLIT_SIZE, REDTASKS, HIS_DATA);
		} else if (args.length == 1) {
			Calendar c = GregorianCalendar.getInstance();
			Date date = new Date();
			c.setTime(date);
			c.add(Calendar.HOUR, -24);
			jobRun(args[0], new SimpleDateFormat("yyyyMMdd").format(c.getTime()), SPLIT_SIZE, REDTASKS, HIS_DATA);
		} else {
			System.out
					.printf("Usage: %s [generic options] <jobname> <inputpath> < split_size(m)> <redtasks>  <his_data>\n   or  <jobname> <inputpath>  ",
							OozieRunner.class.getSimpleName());
		}
	}

	/**
	 * 启动oozie job
	 * 
	 * @param map
	 */
	public static String jobRun(String jobname, String inputpath, String split_size, String redtasks, int hisdata) {
		//Properties prop = ConfigUtils.getConfig(HDFS_CONFIG_PATH);
		String namenode ="hdfs://cloud138:8020";
		String oozieClient = "http://10.95.3.136:11000/oozie/";
		String queuename = "default";
		String jobtracker = "cloud136:8032";

		System.out.println("namenode : " + namenode);
		System.out.println("jobtracker : " + jobtracker);
		System.out.println("oozieClient : " + oozieClient);
		System.out.println("queuename : " + queuename);
		System.out.println("system  user  : tescomm " );
		System.out.println("libpath :  /user/tescomm/rp/oozie/volte_mk" );
		System.out.println("workflow path :  /user/tescomm/rp/oozie/volte_mk" + jobname );
		

	 

		// 启动job
		OozieClient wc = new OozieClient(oozieClient);
		Properties conf = wc.createConfiguration();

		// 只执行一次
		// workflow路径 约定路径为oozie/[jobname]
		conf.setProperty(OozieClient.APP_PATH, namenode + "/user/tescomm/rp/oozie/volte_mk");
		// libpath地址
		conf.setProperty(OozieClient.LIBPATH, namenode + "/user/tescomm/rp/oozie/volte_mk/lib");
         System.out.println(conf.getProperty(OozieClient.APP_PATH));
         System.out.println(conf.getProperty(OozieClient.LIBPATH)+"---------------OozieClient.LIBPATH--"+OozieClient.LIBPATH);
		conf.setProperty("nameNode", namenode);
		conf.setProperty("queueName", queuename);
		conf.setProperty("jobTracker", jobtracker);
		conf.setProperty(OozieClient.USER_NAME, "tescomm");
		conf.setProperty("jobname", jobname);
		conf.setProperty("oozieClient", oozieClient);
		conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
		conf.setProperty("security_enabled","False");
		conf.setProperty("dryrun","False");
		//conf.setProperty("split_size", String.valueOf((Long.valueOf(split_size) * 1024 * 1024)));
		//conf.setProperty("redtasks", redtasks);

		Calendar c = GregorianCalendar.getInstance();
		Date date = new Date();
		c.setTime(date);
		conf.setProperty("sysdate", new SimpleDateFormat("yyyyMMdd").format(c.getTime()));

		c.setTime(date);
		c.add(Calendar.HOUR, -24);
		conf.setProperty("input", inputpath);
		conf.setProperty("yesterday", new SimpleDateFormat("yyyyMMdd").format(c.getTime()));
        System.out.println("==========inputpath=============="+inputpath);
		// 数据输入路径 格式yyyymmddhhmi
		String oozie_id = "";
		try {
			oozie_id = wc.run(conf);
		} catch (OozieClientException e) {
			e.printStackTrace();
		}
		conf.setProperty("oozie_id", oozie_id);
		System.out.println("job_minute启动成功，id : " + oozie_id);
		return oozie_id;
	}
}