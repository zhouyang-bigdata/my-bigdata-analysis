package com.bigdata.hadoop.utils.common.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamUtil
{
	/**
	 * ��byte����ת����InputStream
	 * 
	 * @param in
	 * @return
	 * @throws Exception
	 */
	public static InputStream byte2InputStream(byte[] in) throws IOException {
		ByteArrayInputStream is = new ByteArrayInputStream(in);
		return is;
	}

}
