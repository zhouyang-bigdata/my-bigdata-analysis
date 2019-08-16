package com.bigdata.hadoop.utils.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CSVUtils {
	public static Logger log = LoggerFactory.getLogger(CSVUtils.class);

	private  int recordSize =0 ;
	private  List<List<String>> rowRecordList = new ArrayList<List<String>>();
	
	public  List<List<String>> getRowRecordList() {
		return rowRecordList;
	}

	public  void setRowRecordList(List<List<String>> rowRecordList) {
		this.rowRecordList = rowRecordList;
	}
	
	public  int getRecordSize(){
		return recordSize ;
	}
	
	public  String parserCSVByte2List(byte[] datas){
		String resultStr = "" ;
		Reader inputReader = null;
		InputStream byteInputStream = null;
		byteInputStream = new ByteArrayInputStream(datas);
		BufferedReader reader = null ;
		try {
			inputReader = new InputStreamReader(byteInputStream, "utf-8");
			reader = new BufferedReader(inputReader);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		try {
			String content = "" ;
			List<String> recordList = null ;
			while((content = reader.readLine())!=null){
				
				recordList = makeHeadList(content);
				rowRecordList.add(recordList);
				recordSize ++ ;
				//System.out.println(content);
			}
			System.out.println("csv file's record size = " + recordSize);
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return resultStr ;
	}

	private static List<String> makeHeadList(String content) {
		List<String> headList = new ArrayList<String>();
		String[] heads = null ;
		if(content!=null&&!content.trim().isEmpty()&&content.contains(",")){
			heads = content.split(",");
			headList = Arrays.asList(heads);
		}else{
			headList = Arrays.asList(content);
		}
		return headList;
	}

}

