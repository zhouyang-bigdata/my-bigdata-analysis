package com.bigdata.hadoop.service;

import java.io.FileInputStream;
import java.util.List;
import java.util.Map;

/**
 * @ClassName HDFSHandleService
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/4/9 18:43
 * @Version 1.0
 **/
public interface HDFSHandleService {
    /**
     *
     * @author Administrator
     * @param localFullPath 本地文件全路径 或者可识别的相对路径
     * @param storageFullPath 保存hdfs的全路径
     * @return void
     * @warn 同步上传
     *
     * */
    boolean uploadCSV2HDFS(final String localFullPath, final String storageFullPath);

    /**
     * @param fileFullPath final String
     * @return tmpSavePath String
     * 			" "代表下载失败
     * @warn 使用完毕请记住清理临时文件
     * */
    String downLoadFromHDFS(final String fileFullPath, String tmpFilePath);

    /**
     * 指定下载文件存放路劲
     * @param fileFullPath final String
     * @return tmpSavePath String
     * 			" "代表下载失败
     * @warn 使用完毕请记得清理临时文件
     * */
    String downLoadCSVFromHDFS(final String fileFullPath, final String localStoragePath);

    /**
     *
     * @param fileFullPath
     * @return  fis FileInputStream
     * @warn 请记得及时关闭inputStream以及删除本地临时文件
     *
     * */
    FileInputStream getInputStreamFormHDFS(final String fileFullPath);

    /*
     * @Author zhouyang
     * @Description TODO 获取目录下所有文件信息
     * @Date 13:40 2019/4/8
     * @Param [filePath]
     * @return
     **/
    List<Map<String, String>> getFilesInfoFromPath(final String filePath);
}
