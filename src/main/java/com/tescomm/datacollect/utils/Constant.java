package com.tescomm.datacollect.utils;
/**
 * @author:   刘冰
 * @E-mail:  liubing2013@boco.com.cn
 * @version 
 * 创建时间：2014-8-22 上午10:42:19
 * 说明: 
 */
public interface Constant {
	
	 // 系统常用常量
	public static final String JDBC_CONFIG_PATH = "/config/sys/jdbc_config.properties";
	public static final String REDIS_CONFIG_PATH = "/config/sys/redis_meta_data.properties";
	public static final String HDFS_CONFIG_PATH = "/config/sys/hdfs_config.properties";
	public static final String WATCHER_CONFIF_PATH="/config/sys/watcher_conf.properties";
	public static final String EMPTY = "";
	
	public static final String FTP_CONFIG_PATH = "/config/sys/ftp.properties";
	//表名
	public static final String TABLE_GRID="h_m_gis_cell_query_grid_el";//栅格表
	public static final String TABLE_CELL="h_m_gis_cell_grid_el";//小区表
	public static final String TABLE_SINGLECUSTOMER="i_basic_para_detail";//单用户
	public static final String TABLE_H_CELL_INFO="h_cell_info";//小区的基础数据表
	// redis同步时用到的系统常量
	public static final String ALIAS = ".ORACEL_ALIAS";
	public static final String COLUMNS = ".COLUMNS";
	public static final String KEY_VALUE = ".KEY_VALUE";
	public static final String ORDERS = ".ORDERS";
	public static final String WHERE_CAUSE = ".WHERE_CAUSE";
	public static final String GROUPS = ".GROUPS";
	
	// 临时表的创建语句
	public static final String TMP_TABLES_PATH = "/config/CreateTmpTables.properties";
}
