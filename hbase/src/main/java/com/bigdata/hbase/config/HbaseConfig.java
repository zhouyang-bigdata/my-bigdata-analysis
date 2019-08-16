package com.bigdata.hbase.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * @ClassName HbaseConfig
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/3/21 14:28
 * @Version 1.0
 **/
@Component
@PropertySource({"classpath:application-hbase.yml"})
@ConfigurationProperties(prefix = "")
public class HbaseConfig {
    @Value("${rootdir}")
    private String rootdir;
    @Value("${zookeeper_quorum}")
    private String zookeeperQuorum;
    @Value("${zookeeper_property_clientPort}")
    private String zookeeperClientPort;
    @Value("${tableName}")
    private String tableName;
    @Value("${rowKeyStr}")
    private String rowKeyStr;
    @Value("${qualifierStr}")
    private String qualifierStr;
    @Value("${valueStr}")
    private String valueStr;

    public String getRootdir() {
        return rootdir;
    }

    public void setRootdir(String rootdir) {
        this.rootdir = rootdir;
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    public String getZookeeperClientPort() {
        return zookeeperClientPort;
    }

    public void setZookeeperClientPort(String zookeeperClientPort) {
        this.zookeeperClientPort = zookeeperClientPort;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowKeyStr() {
        return rowKeyStr;
    }

    public void setRowKeyStr(String rowKeyStr) {
        this.rowKeyStr = rowKeyStr;
    }

    public String getQualifierStr() {
        return qualifierStr;
    }

    public void setQualifierStr(String qualifierStr) {
        this.qualifierStr = qualifierStr;
    }

    public String getValueStr() {
        return valueStr;
    }

    public void setValueStr(String valueStr) {
        this.valueStr = valueStr;
    }
}
