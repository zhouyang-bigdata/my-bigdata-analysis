package com.bigdata.hadoop.service.impl;

import com.bigdata.hadoop.service.HDFSHandleService;
import com.bigdata.hadoop.utils.common.utils.FileBytesUtils;
import com.bigdata.hadoop.utils.hadoopUtils.HDFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName HDFSHandleServiceImpl
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/4/9 18:41
 * @Version 1.0
 **/
@Service
public class HDFSHandleServiceImpl implements HDFSHandleService {

    public static Logger LOG = LoggerFactory.getLogger(HDFSHandleServiceImpl.class);

    /**
     *
     * @author Administrator
     * @param localFullPath 本地文件全路径 或者可识别的相对路径
     * @param storageFullPath 保存hdfs的全路径
     * @return void
     * @warn 同步上传
     *
     * */
    public boolean uploadCSV2HDFS(final String localFullPath , final String storageFullPath){
        boolean isDelete = true ;
        try {

            String fileName = localFullPath.substring(localFullPath.lastIndexOf("/")+1,localFullPath.length());
            System.out.println("CSV file name = " + fileName);
            System.out.println("CSV file path = " + localFullPath);
            // 开始处理hdfs文件
            String srcPath = localFullPath; // 本地路径
            String dstPath = storageFullPath; // hdfs存储的逻辑路径
            String sourcePath = "";
            String targetPath = "";
            String fileSize = FileBytesUtils.getFileSize(localFullPath);
            if (Integer.parseInt(fileSize) > 0) {
                if (null != dstPath && !dstPath.isEmpty()) {
                    sourcePath = srcPath ;
                    targetPath = dstPath ;
                    System.out.println("sourcePath = " + sourcePath
                            + " targetPath = " + targetPath);
                    long startMS = System.currentTimeMillis();
                    try{
                        HDFSUtils.uploadFile(sourcePath, targetPath,isDelete);
                    }catch(Exception e){
                        //TODO 把失败任务添加到 失败队列中 并且返回
                        e.printStackTrace();
                        return false;
                    }
                    long useTime = ((System.currentTimeMillis() - startMS));
                    System.out.println("put local csv file 2 HDFS use time "
                            + useTime + " seconds ");
                    //csv2MysqlInfo.setImportTime(useTime);
                    //TODO 上传文件成功后保存信息
                    //this.saveCSVFileMeta2Mysql(csv2MysqlInfo);
                    // 增加上传完成后的删除父目录功能 前提打开了isDelete选项
                    File dirFile = null;
                    if (isDelete) {
                        dirFile = new File(srcPath);
                        if (dirFile.exists() && dirFile.isDirectory()
                                && dirFile.listFiles().length == 0) {
                            System.out.println("that dirFile has delete.."
                                    + srcPath);
                            dirFile.delete();
                        }
                    }
                }
            }
            else{
                return false ;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return true ;
    }

    /**
     * @param fileFullPath final String
     * @return tmpSavePath String
     * 			" "代表下载失败
     * @warn 使用完毕请记住清理临时文件
     * */
    public String downLoadFromHDFS(final String fileFullPath, String tmpFilePath){
        String tmpSavePath = "" ;
        String fileName = "" ;
        boolean downLoadsucceed = false ;
        fileName = fileFullPath.substring(fileFullPath.lastIndexOf("/")+1, fileFullPath.length());
        tmpSavePath = tmpFilePath+"/"+fileName ;
        try {
            downLoadsucceed = HDFSUtils.downLoadFile(fileFullPath, tmpSavePath);
            String info = "是否下载成功：" + downLoadsucceed;
            LOG.info(info);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(downLoadsucceed){
            return tmpSavePath ;
        }else{
            return "";
        }
    }

    /**
     * 指定下载文件存放路劲
     * @param fileFullPath final String
     * @return tmpSavePath String
     * 			" "代表下载失败
     * @warn 使用完毕请记得清理临时文件
     * */
    public String downLoadCSVFromHDFS(final String fileFullPath,final String localStoragePath){
        String fileName = "" ;
        boolean downLoadsucceed = false ;
        String localStorageFullPath = localStoragePath+"/"+fileName ;
        fileName = fileFullPath.substring(fileFullPath.lastIndexOf("/")+1,fileFullPath.length());
        try {
            downLoadsucceed = HDFSUtils.downLoadFile(fileFullPath,localStorageFullPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(downLoadsucceed){
            return localStorageFullPath ;
        }else{
            return "";
        }
    }

    /**
     *
     * @param fileFullPath
     * @return  fis FileInputStream
     * @warn 请记得及时关闭inputStream以及删除本地临时文件
     *
     * */
    public FileInputStream getInputStreamFormHDFS(final String fileFullPath){
        boolean downLoadsucceed = false ;
        String tmpFilePath = "/tmp" ;
        String fileName = "" ;
        String tmpSavePath = tmpFilePath + "/" + fileName ;
        fileName = fileFullPath.substring(fileFullPath.lastIndexOf("/")+1,fileFullPath.length());
        FileInputStream fis = null ;
        try {
            downLoadsucceed = HDFSUtils.downLoadFile(fileFullPath,tmpSavePath);
            fis = new FileInputStream(new File(tmpSavePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(downLoadsucceed){
            return fis ;
        }else{
            return null;
        }
    }

    /*
     * @Author zhouyang
     * @Description TODO 获取目录下所有文件信息
     * @Date 13:40 2019/4/8
     * @Param [filePath]
     * @return
     **/
    public List<Map<String, String>> getFilesInfoFromPath(final String filePath){
        //
        List<Map<String, String>> fileInfoList = new ArrayList<>();
        try {
            fileInfoList = HDFSUtils.getFilesInfoFromPath(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileInfoList;
    }

}
