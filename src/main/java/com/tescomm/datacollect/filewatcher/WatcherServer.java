package com.tescomm.datacollect.filewatcher;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.tescomm.datacollect.utils.CustomFileUtils;
import com.tescomm.datacollect.utils.DateUtils;
import com.tescomm.zk.ZkConnection;
import com.tescomm.zk.utils.Metadata;
/**
 * 监控目录
 * @author admin
 */
public class WatcherServer extends Observable {

	public static Logger logger = Logger.getLogger(WatcherServer.class);

	private static Pattern pattern = Pattern.compile("\\,");

	private WatchService watcher;
	private Path path;
	private WatchKey key;
	private Executor executor = Executors.newSingleThreadExecutor();
	// 监控的根目录
	public static String watcher_path;
	/**
	 * 监控的目录, 与根目录组成完成的监控路径 适用于多目录监控
	 */
	private static String watcher_files;
	// hdfs目录
	public static String dest_path;
	// 上传的目录, 与dest_path目录组成完整的上传目录
	private static String dest_files;
	
	// 目的目录分区　　　０－不分区　　２４－天　　　６０－小时　５／１０／１５／２０／３０－＿分钟，　与dest_files成对出现
	private static String dest_files_part;

	private static Map<String, String> src2dest;
	private static Map<String, String> dest2part;
	// 文件备份目录
	private static String back_path;
	// 错误文件目录
	private static String error_path;
	// 监控文件后缀
	private static String suffix;
	// 效验文件后缀
	private static String check_suffix;

	public static BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
	String src;
	String dest;
	String watcher_file;
	String part;
	boolean flag = true;
	long time;
	int sum;

	/**
	 * FutureTask是为了弥补Thread的不足而设计的，它可以准确地知道线程什么时候执行完成并获得到线程执行完成后返回的结果
	 */
	FutureTask<Integer> task = new FutureTask<Integer>(new Callable<Integer>() {
		public Integer call() throws InterruptedException {
			processEvents();
			return Integer.valueOf(0);
		}
	});

	/**
	 * java FileWatcher 实现目录监控
	 * 
	 * @param event
	 * @return
	 */
	@SuppressWarnings("unchecked")
	static <T> WatchEvent<T> cast(WatchEvent<?> event) {
		return (WatchEvent<T>) event;
	}

	/**
	 * 初始化 获取监控的路径
	 */
	static {
		// 从配置文件读取zk server信息
		Metadata m = new Metadata("/config/zookeeper_conf.properties");
		logger.info(m.getValue("namespace"));
		logger.info(m.getValue("ip"));
		logger.info(m.getValue("port"));
		System.out.println("---namespace---"+m.getValue("namespace"));
		// 创建zk连接
		ZkConnection zkc = new ZkConnection(m.getValue("namespace"), m.getValue("ip"), Integer.valueOf(m.getValue("port")));
		CuratorFramework client = zkc.getZKConnection();

		try {
			client.start();
			Stat stat = new Stat();
			// 从ｚｋ中得到配置信息
			watcher_path = new String(client.getData().storingStatIn(stat).forPath("/watcher_path")).replaceAll("\"", "");
			watcher_files = new String(client.getData().storingStatIn(stat).forPath("/watcher_files")).replaceAll("\"", "");
			dest_path = new String(client.getData().storingStatIn(stat).forPath("/dest_path")).replaceAll("\"", "");
			dest_files = new String(client.getData().storingStatIn(stat).forPath("/dest_files")).replaceAll("\"", "");
			dest_files_part = new String(client.getData().storingStatIn(stat).forPath("/dest_files_part")).replaceAll("\"", "");
			back_path = new String(client.getData().storingStatIn(stat).forPath("/back_path")).replaceAll("\"", "");
			error_path = new String(client.getData().storingStatIn(stat).forPath("/err_path")).replaceAll("\"", "");
			check_suffix = new String(client.getData().storingStatIn(stat).forPath("/check_suffix")).replaceAll("\"", "");
			suffix = new String(client.getData().storingStatIn(stat).forPath("/suffix")).replaceAll("\"", "");
              
			
			System.out.println("---watcher_path-"+watcher_path);
			System.out.println("---watcher_files-"+watcher_files);
			System.out.println("---dest_path-"+dest_path);
			System.out.println("---dest_files-"+dest_files);
			
			
			// 创建监控目录文件夹与目的目录文件夹的对应map
			String[] src = pattern.split(watcher_files);
			String[] dest = pattern.split(dest_files);
			String[] part = pattern.split(dest_files_part);
			// 效验监控目录文件夹数量与目的目录文件夹数量是否一致
			if (src.length != dest.length) {
				logger.error("监控目录文件夹数量与目的目录文件夹数量不相符. ");
				logger.error("监控目录文件夹数量: " + src.length + ".  " + watcher_files);
				logger.error("目的目录文件夹数量: " + dest.length + ".  " + dest_files);
				System.exit(0);
			} else if(dest.length != part.length){  // 校验目的目录与分区数是否一致
				logger.error("目的目录文件夹数量与分区数量不相符. ");
				logger.error("目的目录文件夹数量: " + dest.length + ".  " + dest_files);
				logger.error("分区数量: " + part.length + ".  " + dest_files_part);
				System.exit(0);
			} else {
				src2dest = new HashMap<String, String>();
				dest2part = new HashMap<String, String>();
				for (int i = 0; i < src.length; i++) {
					src2dest.put(src[i], dest[i]);
					dest2part.put(dest[i], part[i]);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			client.close();
		}

	}

	/**
	 * 注册监控事件
	 * 
	 * @param src
	 *            源目录, 待监控的目录
	 * @param dest
	 *            目的目录
	 * @throws IOException
	 */
	public WatcherServer(String src, String dest, String watcher_file, String part) throws IOException {
		this.src = src;
		this.dest = dest;
		this.watcher_file = watcher_file;
		this.part = part;
		watcher = FileSystems.getDefault().newWatchService();
		// 监控目录内文件的更新、创建和删除事件
		path = Paths.get(src);
		// ENTRY_CREATE: 注册创建任务
		key = path.register(watcher, ENTRY_CREATE);
	}

	/**
	 * 启动监控过程
	 */
	public void execute() {
		// 通过线程池启动一个额外的线程加载Watching过程
		executor.execute(task);
	}

	/**
	 * 关闭后的对象无法重新启动
	 * 
	 * @throws IOException
	 */
	public void shutdown() throws IOException {
		watcher.close();
		executor = null;
	}

	/**
	 * 启动监控前初始化目录下所有文件
	 */
	public static void readFile(String path, String filepath, String dest) {
		logger.info("查看监控目录:" + path + ", 是否已存在文件.");
		File file = new File(path);
		// 文件名不是为.ok的文件 初始认为是错误文件
		List<String> errorFileList = new ArrayList<String>();
		// 保存 .ok文件对应的.csv文件 以防被认为错误文件
		List<String> csvFileList = new ArrayList<String>();
		// 已经读到的文件列表
		File[] fileList = file.listFiles();
		String name = "";
		logger.info("已存在文件数量: " + fileList.length);
		// 判断是否存在文件
		if (fileList != null) {
			for (File fi : fileList) {
				// 判断是文件还是文件夹
				if (fi.isFile()) {  // 错了文件
					name = fi.getName();
					// 判断文件后缀是否是待监控文件
					if (name.toString().endsWith(check_suffix)) {
						// 根据校验文件拼接数据文件名称
						String filename = name.toString().substring(0, name.toString().indexOf(".")) + suffix;
						// 根据得到的校验文件, 判断数据文件是否存在
						if (new File(mergeString(path, filename)).exists()) {
							// 将数据文件保存到临时列表
							csvFileList.add(filename);
							//　将文件放到数据put的队列中
							queue.add(path  + filename + "," + filepath);
						} else {
							// 数据文件不存在, 放入到错误日志列表, 等待处理
							errorFileList.add(name);
							logger.info(path   + filename + " 文件不存在!");
						}
					} else {
						// 将文件夹放入到错误日志列表, 等待处理
						errorFileList.add(name);
					}
				}
			}
			// 清除错误文件
			String date = DateUtils.dateToyyyymmss(new Date());
			for (String errorFile : errorFileList) {
				logger.info("开始移除错误文件, 文件名称: " + path   + errorFile);
				// 判断文件是否存在
				if (new File(mergeString(path, errorFile)).exists() && !csvFileList.contains(errorFile)) {
					try {
						CustomFileUtils.moveFile(mergeString(path, errorFile), mergeString(error_path, date, filepath));
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {
					logger.info(path + errorFile + "文件不存在!");
				}
			}
		}
	}

	/**
	 * 监控文件系统事件
	 */
	void processEvents() {
		logger.info("Thread name: " + Thread.currentThread().getName() + ", 监控目录: " + src + ", 目的目录: " + dest);
		while (true) {
			// 等待直到获得事件信号
			WatchKey signal;
			try {
				signal = watcher.take();
			} catch (InterruptedException x) {
				return;
			}

			for (WatchEvent<?> event : signal.pollEvents()) {
				Kind<?> kind = event.kind();

				if (kind == OVERFLOW) {
					continue;
				}

				WatchEvent<Path> ev = cast(event);
				Path name = ev.context();

				if (kind == ENTRY_CREATE) {
					if (flag) {
						logger.info("started.....");
						time = System.currentTimeMillis();
						sum = 0;
						flag = false;
					}
					try {
						// 判断是否是效验文件
						if (name.toString().endsWith(check_suffix)) {
							// 将捕获到的效验文件名替换为待上传的文件名
							String filename = name.toString().substring(0, name.toString().indexOf(".")) + suffix;
							// 判断文件是否存在
							if (new File(src  + filename).exists()) {
								// 将文件加入消息队列
								logger.info(src  + filename + ",watcher_file:" + watcher_file);
								System.out.println(src  + filename + ",watcher_file:" + watcher_file);
								queue.add(src   + filename + "," + watcher_file);
							} else {
								logger.info(src + filename + "文件不存在!");
							}
						}
//						System.out.println("发现文件文件：111 " + name.toString());

					} catch (Exception e) {
						e.printStackTrace();
					}

				} else if (kind == ENTRY_DELETE) {
					logger.info("删除文件： " + name.toString());
				} else if (kind == ENTRY_MODIFY) {
					logger.info("修改文件： " + name.toString());
				}
			}
			// 为监控下一个通知做准备
			key.reset();
		}
	}

	public static void WatcherServers() throws Exception {
		String[] watcher_file = pattern.split(watcher_files);
		String[] dest_file = pattern.split(dest_files);
		String[] dest_files_part_s = pattern.split(dest_files_part);

		for (int i = 0; i < watcher_file.length; i++) {
			// 监控多目录是读取目录
			// new Thread(new QueueFileToHdfs(suffix, check_suffix, back_path,
			// watcher_file[i])).start();
			readFile(mergeString(watcher_path, watcher_file[i]), watcher_file[i], mergeString(dest_path, dest_file[i]));
			logger.info("Thread name: " + Thread.currentThread().getName() + ", 监控目录: " + mergeString(watcher_path, watcher_file[i]) + ", 目的目录: " + mergeString(dest_path, dest_file[i]));
			WatcherServer ws = new WatcherServer(mergeString(watcher_path, watcher_file[i]), mergeString(dest_path, dest_file[i]), watcher_file[i], dest_files_part_s[i]);
			ws.execute();
			// 启动监控
		}

	}

	/**
	 * 本类公用方法, 合并路径, 并添加路径分隔符
	 * 
	 * @param ss
	 * @return
	 */
	private static String mergeString(String... ss) {
		StringBuffer sb = new StringBuffer();
		for (String s : ss) {
			sb.append(s).append("/");
		}
		return sb.toString();
	}

	/**
	 * 监控入口函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			// 注册监控
			WatcherServer.WatcherServers();
			// 启动监控
			new Thread(new QueueFileToHdfs(suffix, check_suffix, back_path, dest_path, src2dest, dest2part)).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
