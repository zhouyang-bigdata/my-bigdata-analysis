package com.bigdata.hadoop.utils.hadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author libangqin
 * @date 2017-08-15 hdfs基本操作类
 */
public class HDFSUtils {
	public static Logger log = LoggerFactory.getLogger(HDFSUtils.class);

	// 通过流方式创建新文件
	public static void createFile(String dst, byte[] contents, String hdfsPath) throws IOException {
		Configuration conf = getHDFSConf();
		FileSystem fs = FileSystem.newInstance(conf);
		Path dstPath = new Path(dst); // 目标路径
		// 打开一个输出流
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(contents);
		outputStream.close();
		fs.close();
		log.info("文件创建成功！");
	}

	// 从本地文件读取到hdfs
	public static void createFile(String inputDirStr, String hdfsFileStr) throws IOException {

		Configuration conf = getHDFSConf();
		// String dst = "hdfs://192.168.46.16:8020/user/dell" ;
		FileSystem hdfs = FileSystem.newInstance(conf);
		FileSystem local = FileSystem.getLocal(conf);
		Path inputDir = new Path(inputDirStr);
		Path hdfsFile = new Path(hdfsFileStr);
		FileStatus[] inputFiles = local.listStatus(inputDir);
		FSDataOutputStream out = hdfs.create(hdfsFile);
		for (int i = 0; i < inputFiles.length; i++) {
			log.info(inputFiles[i].getPath().getName());
			FSDataInputStream in = local.open(inputFiles[i].getPath());
			byte buffer[] = new byte[256];
			int byteRead = 0;
			while ((byteRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, byteRead);
			}
			in.close();
		}
		out.close();
		local.close();
		hdfs.close();

	}

	@Test
	public void testCreateFile() {
		String inputDirStr = "D://test//20160417_NS7227_7227_KBFI_PHNL.csv";
		String hdfsFileStr = "/test/20160417_NS7227_7227_KBFI_PHNL.csv";
		try {
			HDFSUtils.createFile(inputDirStr, hdfsFileStr);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCreateFileByStream() {
		// String inputDirStr = "D://test//20160417_NS7227_7227_KBFI_PHNL.csv" ;
		String hdfsFileStr = "/test/20160417_NS7227_7227_KBFI_PHNL.csv";
		String hdfsPath = "hdfs://10.0.0.222:9000";
		String contents = "helloWorlds";
		try {
			HDFSUtils.createFile(hdfsFileStr, contents.getBytes(), hdfsPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String getOsType() {
		
		String osTypeStr = System.getProperty("os.name");
		if (osTypeStr.toLowerCase().startsWith("win")) {
			osTypeStr = "window";
		} else {
			osTypeStr = "linux";
		}
		return osTypeStr;
	}

	/**
	 * @Description TODO 获取hdfs 配置
	 * @param
	 * @author Administrator
	 * @return Configuration 通过HDFS的配置文件获取configuration对象
	 * @warn 提示需要将集群上的core-site.xml 以及hdfs-site.xml文件放到项目资源目录中
	 * 
	 */
	public static Configuration getHDFSConf() {

		Configuration conf = new Configuration();
		try {
			String hdfsCoreResourcePath = HDFSUtils.class.getClassLoader().getResource("core-site.xml").getPath();
			String hdfsSiteResourcePath = HDFSUtils.class.getClassLoader().getResource("hdfs-site.xml").getPath();

			// 通过配置文件获取这两个hadoop的配置文件 首先判断操作系统
			String coreSitePath = "" ;
			String hdfsSitePath = "" ;
			if(HDFSUtils.getOsType().equals("window")) {
				coreSitePath = hdfsCoreResourcePath.substring(hdfsCoreResourcePath.indexOf("/") + 1,hdfsCoreResourcePath.length());
				hdfsSitePath = hdfsSiteResourcePath.substring(hdfsSiteResourcePath.indexOf("/") + 1,hdfsSiteResourcePath.length());
			}else {
				coreSitePath = hdfsCoreResourcePath ;
				hdfsSitePath = hdfsSiteResourcePath ;
			}
			log.info("core-site.xml 's file path = " + coreSitePath);
			File file = new File(coreSitePath);
			log.info("is file? " + file.isFile());
			conf.addResource(new Path(coreSitePath));
			conf.addResource(new Path(hdfsSitePath));
			//conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", LocalFileSystem.class.getName());
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		return conf;
	}

	/**
	 * 删除文件夹或者目录
	 *
	 * @param targetPath
	 *            hdfs路径
	 * @param recursive
	 *            是否递归删除文件夹下的面的文件
	 * @return 返回删除结果
	 */
	public static boolean delete(final String targetPath, boolean recursive) {
		FileSystem fs = null;
		boolean deleteSuccessFlag = false;
		try {
			Path filePath = new Path(targetPath);
			Configuration conf = getHDFSConf();
			fs = FileSystem.newInstance(conf);
			deleteSuccessFlag = fs.delete(filePath, recursive);
		} catch (Exception e) {
			e.printStackTrace();
			log.info("delete that file failed and path=$path msg:${ex.getMessage}");
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return deleteSuccessFlag;
	}

	/**
	 * @param hdfsFullPath
	 *            hdfs逻辑全路径
	 * @param downLoadPath
	 *            本地保存路径
	 * @return 下载成功标志
	 */
	public static boolean downLoadFile(String hdfsFullPath, String downLoadPath) throws IOException {

		boolean downLoadSuccessed = false;
		OutputStream output = null;
		FSDataInputStream in = null;
		if (null == downLoadPath || downLoadPath.isEmpty() || null == hdfsFullPath || hdfsFullPath.isEmpty()) {
			downLoadSuccessed = false;
			return downLoadSuccessed;
		} else {
			output = new FileOutputStream(downLoadPath);
		}
		Configuration conf = getHDFSConf();
		FileSystem fs = null;
		Path srcPath = new Path(hdfsFullPath);
		try {
			fs = FileSystem.newInstance(conf);
			in = fs.open(srcPath);
			IOUtils.copyBytes(in, output, 4096, true); // 复制到标准输出流
			//下载成功
			downLoadSuccessed = true;
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return downLoadSuccessed;
		} finally {
			if (in != null) {
				IOUtils.closeStream(in);
			}
			if (output != null) {
				output.close();
			}
			if (fs != null) {
				fs.close();
			}
		}
		return downLoadSuccessed;
	}

	/**
	 * @author libangqin
	 * @throws InterruptedException
	 * @date 2017-08-15 根据传入的参数上传本地文件到hdfs指定目录中 isDelete决定是否删除本地文件
	 */
	public static void uploadFile(String src, String dst, boolean isDelete) {
		Configuration conf = getHDFSConf();
		FileSystem fs = null;
		Path srcPath = new Path(src); // 原路径
		Path dstPath = new Path(dst); // 目标路径
		// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
		// 打印文件路径
		File file = new File(src);
		try {
			FileSystem.newInstance(conf);
			if (!file.exists()) {
				log.warn("that local file " + src + "is not exist!");
			} else {
				if (null == fs) {
					fs = FileSystem.get(conf);
				}
				FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
				fs.copyFromLocalFile(isDelete, srcPath, dstPath);
				fs.setPermission(dstPath, permission);
				log.info("Upload to " + conf.get("fs.default.name"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void testUploadFile() {
		// 如果是监听目录就会发生删除目录的情况 指定文件则可以避免
		String srcPath = "D://V-1001_20170101100310.csv";
		String dstPath = "/testUpload/V-1001_20170101100310.csv";
		boolean isDelete = true;
		HDFSUtils.uploadFile(srcPath, dstPath, isDelete);

	}

	/**
	 * @author libangqin 根据参数创建目录
	 * 
	 */
	public static void mkdir(String path) throws IOException {
		Configuration conf = getHDFSConf();
		FileSystem fs = FileSystem.newInstance(conf);
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			log.info("create dir ok!");
		} else {
			log.warn("create dir failure");
		}
		fs.close();
	}

	public static boolean checkFileSize(String filePath, int fileSize) {

		boolean checkable = false;
		FileSystem fs = null;
		try {
			int totalFileSize = 0;
			Configuration conf = getHDFSConf();
			fs = FileSystem.newInstance(conf);
			totalFileSize = (int) fs.getContentSummary(new Path(filePath)).getLength();
			log.info("use hdfs api get file's size = " + totalFileSize);
			// 如果查询的文件大小 大于真实hdfs中存储的文件大小意味着文件入库不准确
			if (fileSize > totalFileSize) {
				checkable = false;
				log.warn("what your check file : " + filePath + "fileSize = " + fileSize
						+ "is not complete import to hdfs...");
				log.info("what your check file : " + filePath + "fileSize = " + fileSize
						+ "is not complete import to hdfs...");
			} else {
				checkable = true;
			}
		} catch (IOException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					log.error(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		return checkable;
	}

	/**
	 * 
	 * @param filePath
	 *            String hdfs文件的逻辑路径
	 * @param fileSize
	 *            int 文件大小
	 * @return byte[] 比特流
	 * 
	 */
	public static byte[] readFileToByte(String filePath, int fileSize) throws IOException {
		FileSystem fs = null;
		FSDataInputStream in = null;
		try {
			byte[] qarBuf = new byte[fileSize];
			Configuration conf = getHDFSConf();
			fs = FileSystem.newInstance(conf);
			in = fs.open(new Path(filePath));
			int leftSize = fileSize;
			int readSize = 0;
			while (leftSize > 0) {
				int nSize = in.read(qarBuf, readSize, 4096);
				readSize += nSize;
				leftSize -= nSize;
			}
			return qarBuf;
		} catch (Exception ex) {
			log.error(ex.getMessage());
			ex.printStackTrace();
			return new byte[0];
		} finally {
			try {
				if (in != null) {
					in.close();
				}
				if (fs != null) {
					fs.close();
				}
			} catch (Exception e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}

	}

	// 读取文件的内容到标准输出
	public static void readFile(String filePath) throws IOException {
		int fileSize = 1000000;
		OutputStream outputStream = new ByteArrayOutputStream();
		byte[] qarBuf = new byte[fileSize];
		Configuration conf = getHDFSConf();
		FileSystem fs = FileSystem.newInstance(conf);
		Path srcPath = new Path(filePath);
		InputStream in = null;
		try {
			in = fs.open(srcPath);
			IOUtils.copyBytes(in, outputStream, 4096, false); // 复制到标准输出流
			outputStream.write(qarBuf, 0, fileSize);
		} finally {
			IOUtils.closeStream(in);
		}
	}

	@Test
	public void testGetFileSize() {

		String fileName = "/kxnf/next.csv";
		try {
			log.info("the file next.csv 's fileSize = " + getFileSize(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static long getFileSize(String fileName) throws IOException {
		long fileSize = 0L;
		Configuration conf = getHDFSConf();
		FileSystem fs = FileSystem.newInstance(conf);
		Path check = null;
		if (isExist(fileName)) {
			check = new Path(fileName);
		}
		fileSize = fs.getFileStatus(check).getLen();
		if (null != fs) {
			fs.close();
		}
		return fileSize;
	}

	/**
	 * @author libangqin
	 * @date 2017-8-15 判断要查找的文件在hdfs指定的路径中是否存在
	 * 
	 */
	public static boolean isExist(String fullPath) throws FileNotFoundException, IOException {
		boolean existFlag = false;
		FileSystem fs = null;
		try {
			Configuration conf = getHDFSConf();
			fs = FileSystem.newInstance(conf);
			Path check = new Path(fullPath);
			existFlag = fs.exists(check);
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
			return false;
		} finally {
			if (null != fs) {
				fs.close();
			}
		}
		return existFlag;
	}

	/**
	 * @author libangqin
	 * @date 2017-8-15 判断要查找的文件在hdfs指定的路径中是否存在
	 * 
	 */
	public static boolean isExist(String fileName, String checkPath) throws FileNotFoundException, IOException {
		boolean existFlag = false;
		Configuration conf = getHDFSConf();
		FileSystem fs = FileSystem.newInstance(conf);
		Path check = new Path(checkPath);
		FileStatus[] inputFiles = fs.listStatus(check);
		if (inputFiles == null) {
			throw new IOException(" the path is not correct:" + checkPath);
		}
		for (int i = 0; i < inputFiles.length; i++) {
			if (inputFiles[i].isDirectory()) {
				continue;
			}
			if (inputFiles[i].getPath().getName().equals(fileName)) {
				existFlag = true;
				break;
			}
		}
		if (null != fs) {
			fs.close();
		}
		return existFlag;

	}

	@Test
	public void testExist() {
		String fileName = "20160417_NS7227_7227_KBFI_PHNL.csv";
		String dstPath = "/kxnf/2017/08/15/";
		try {
			if (HDFSUtils.isExist(fileName, dstPath)) {
				log.info("that file is exist!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/*
	 * @Author zhouyang
	 * @Description TODO 获取目录下所有文件信息
	 * @Description TODO hdfs FileSystem 对象,path 文件路径
	 * @Date 12:00 2019/4/8
	 * @Param [hdfs, path]
	 * @return
	 **/
	public static List<Map<String, String>> getFilesInfoFromPath(String filePath) throws IOException {
		Configuration conf = getHDFSConf();
		FileSystem fs = FileSystem.newInstance(conf);
		//
		List<Map<String, String>> fileInfoList = new ArrayList<>();
		try{
			if(fs == null || filePath == null){
				return null;
			}
			Path path = new Path(filePath);
			//获取文件列表
			FileStatus[] files = fs.listStatus(path);

			//展示文件信息
			for (int i = 0; i < files.length; i++) {
				try{
					if(files[i].isDirectory()){
						//如果目录下还有目录，返回null
						String errInfo = "目录下还有目录";
						log.error(errInfo);
						log.error(">>>" + files[i].getPath()
								+ ", dir owner:" + files[i].getOwner());
						//递归调用
						//getFilesInfoFromPath(files[i].getPath());
						return null;
					}else if(files[i].isFile()){
						log.info("path:" + files[i].getPath());
						log.info("length:" + files[i].getLen());
						log.info("owner:" + files[i].getOwner());

						Map<String, String> fileMap = new HashMap<>();
						fileMap.put("path", files[i].getPath().toString());
						fileMap.put("length", String.valueOf(files[i].getLen()));
						fileMap.put("owner", files[i].getOwner());
						fileInfoList.add(fileMap);
					}
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return fileInfoList;
	}

	public static void main(String[] args){
		String path = "/test-data/vehicleData/output/20190201";
		//
		List<Map<String, String>> fileInfoList = new ArrayList<>();
		try{
			fileInfoList = getFilesInfoFromPath(path);
		}catch (Exception e){

		}

	}
}
