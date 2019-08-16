package com.bigdata.hadoop.utils.common.utils;
import java.util.Random;

public class RdmUtil {

	/**
	 * 
	 */
	public RdmUtil() {
		// TODO Auto-generated constructor stub
	}

	private static Random random = new Random();
	
	public static String getRandomStr(int startNum, int endNum) {
		int r = getRandomNum(startNum,  endNum);
		return Integer.toString(r);
	}

	public static int getRandomNum(int startNum, int endNum) {
		int i = endNum - startNum;
		int r = random.nextInt(i) + startNum;
		return r;
	}
	
	public static String getRandomHexStr(int strartNum, int endNum){
		int i = (int) getRandomNum(strartNum,endNum);
		String str = Integer.toHexString(i);
		return str;
	}
	
	public static String getRandomMac(String str){
		StringBuffer sb = new StringBuffer(str);
		while(sb.length() < 17){
			if(sb.length()==2 || sb.length()==5 ||sb.length()==8 ||sb.length()==11 || sb.length()==14){
				sb = sb.append(":");
			}
			sb = sb.append(getRandomHexStr(0,15));
		}
		return sb.toString();
	}
	
}
