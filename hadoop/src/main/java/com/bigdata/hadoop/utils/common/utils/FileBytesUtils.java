package com.bigdata.hadoop.utils.common.utils;

import java.io.*;

public class FileBytesUtils
{
	
	 //获取文件的byte[] 流 
    public static byte[] getBytes(String filePath){  
        byte[] buffer = null;  
        try {  
            File file = new File(filePath);  
            FileInputStream fis = new FileInputStream(file);  
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);  
            byte[] b = new byte[1000];  
            int n;  
            while ((n = fis.read(b)) != -1) {  
                bos.write(b, 0, n);  
            }  
            fis.close();  
            bos.close();  
            buffer = bos.toByteArray();  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return buffer;  
    }  
  
 // 将byte流输出到文件
 	public static String getFileSize(String dstFilePath) {
 		String fileSize = "";
 		File file = null;
 		try {
 			file = new File(dstFilePath);
 			if (!file.exists() && file.isDirectory()) {// 判断是否是文件夹
 				file.mkdirs();
 				System.out.println("there not exist that file" + dstFilePath);
 			} else {
 				fileSize = String.valueOf(file.length());
 			}
 		} catch (Exception e) {
 			e.printStackTrace();
 		}
 		return fileSize;
 	}
    //将byte流输出到文件
    public static void getFile(byte[] bfile, String filePath,String fileName) {  
        BufferedOutputStream bos = null;  
        FileOutputStream fos = null;  
        File file = null;  
        try {  
            File dir = new File(filePath);  
            if(!dir.exists()&&dir.isDirectory()){//判断是否是文件夹  
                dir.mkdirs();  
            }  
            file = new File(filePath+"\\"+fileName);  
            fos = new FileOutputStream(file);  
            bos = new BufferedOutputStream(fos);  
            bos.write(bfile);  
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            if (bos != null) {  
                try {  
                    bos.close();  
                } catch (IOException e1) {  
                   e1.printStackTrace();  
                }  
            }  
            if (fos != null) {  
                try {  
                    fos.close();  
                } catch (IOException e1) {  
                    e1.printStackTrace();  
                }  
            }  
        }  
    } 
}
