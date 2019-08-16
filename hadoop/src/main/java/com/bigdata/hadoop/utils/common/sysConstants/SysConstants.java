package com.bigdata.hadoop.utils.common.sysConstants;


import com.myhadoop.utils.common.utils.URLUtils;
import org.junit.Test;

/**
 * @author libangqin
 * @date 2017-08-14 
 * 一些全局变量配置
 * 如配置文件指定名为config.properties
 * */
public class SysConstants {
	public static final String configPath = URLUtils.getURLDecoderString(Thread.currentThread().getContextClassLoader().getResource("Hbase_Query_Config.properties").getPath());
	public static final String kafka_zookeeper_session_timeout_ms = "6000";
	public static final String kafka_zookeeper_sync_time_ms = "200";
	public static final String kafka_auto_commit_enable = "true";
	public static final String kafka_auto_commit_interval_ms = "5000";
	public static final String kafka_buffer_memory = Long
			.toString(33554432 * 5);

	public static final int kafka_message_threadCount = 1;
	public static final String kafka_message_topic = "topic1"; // "topic1";wifi-topic
	public static final String fetch_message_max_bytes = "10485760"; // 10M
	public static final String rebalance_max_retries = "5";
	public static final String rebalance_backoff_ms = "1300";
	public static final int DECODE_STATUS_IN_KAFKA_TYPE = 1;
	public static final int DECODE_STATUS_IN_MEMORY_TYPE = 3;
	public static final int RPC_SERVER_TYPE = 2;
	public static final int DEFAULT_DATA_VERSION = 1;
	//任务状态
	public static final String JOB_FINISH_STATUS = "1";
	public static final String JOB_RUNNING_STATUS = "2";
	public static final String JOB_ERROR_STATUS = "3";
	
	public static final int MAX_KAFKA_QUEUE = 1000 ;
	public static final int max_msg_nums = 5000 ;
	public static final int DEFAULT_SCAN_INTERVAL = 100;
	
	@Test
	public void testGetPath() {
		String testPath = "C:\\Program Files (x86)\\MyDrivers";
		System.out.println("path = " + URLUtils.getURLDecoderString(testPath));
		
	}
	
}
