package com.tescomm.datacollect.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * @author: 刘冰
 * @E-mail: liubing2013@boco.com.cn
 * @version 创建时间：2015-1-5 上午10:52:23 说明:
 */
public class DateUtils {

	private static DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
	private static DateFormat df2 = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * 获取系统时间, 并按要求格式化
	 * @param pattern
	 * @return
	 */
	public static String systemDate(String pattern){
		return date2String(new Date(), pattern);
	}
	
	/**
	 * 日期按照传入格式转换成字符串
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String date2String(Date date, String pattern) {
		SimpleDateFormat dformat = new SimpleDateFormat(pattern);
		return dformat.format(date);
	}
	/**
	 * 将日期转换成String
	 * 
	 * @param date
	 * @return
	 */
	public static String dateToyyyymmss(Date date) {
		String str = df2.format(date);
		return str;
	}
	/**
	 * 计算与当前天相差的天数, 并按要求格式化
	 * 
	 * @param differnum
	 * @return
	 */
	public static String differ2day(int differnum, String pattern) {
		Calendar c = GregorianCalendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.HOUR_OF_DAY, differnum * 24);
		return date2String(c.getTime(), pattern);
	}

	/**
	 * String转换成date
	 * 
	 * @param date_time
	 * @return
	 */
	public static Date StringToDate(String date_time) {
		Date date = null;
		try {
			date = df1.parse(date_time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	/**
	 * 获取当前时间的前一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getNextDay() {
		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		date = calendar.getTime();
		return date;
	}

	public static void main(String[] args) {
		System.out.println(systemDate("yyyyMMddhh"));
		System.out.println(differ2day(-1, "yyyyMMdd"));
	}

}
