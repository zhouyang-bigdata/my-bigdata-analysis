package com.bigdata.hadoop.utils.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPUtils {

	private static String getHostNameForLiunx() {
		try {
			return (InetAddress.getLocalHost()).getHostName();
		} catch (UnknownHostException uhe) {
			String host = uhe.getMessage(); // host = "hostname: hostname"
			if (host != null) {
				int colon = host.indexOf(':');
				if (colon > 0) {
					return host.substring(0, colon);
				}
			}
			return "UnknownHost";
		}
	}

	public static String getIPAddress() throws UnknownHostException {
		InetAddress address = InetAddress.getLocalHost();// 获取的是本地的IP地址
															// //PC-20140317PXKX/192.168.0.121
		String hostAddress = address.getHostAddress();// 192.168.0.121
		return hostAddress;
	}

	public static String getHostName() {
		if (System.getenv("COMPUTERNAME") != null) {
			return System.getenv("COMPUTERNAME");
		} else {
			return getHostNameForLiunx();
		}
	}

	public static String getComputedID() throws UnknownHostException {
		String result = "";
		String address = getIPAddress();
		result = address + "_" + getHostName();
		return result;
	}
}
