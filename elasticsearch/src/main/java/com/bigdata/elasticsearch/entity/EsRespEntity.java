package com.bigdata.elasticsearch.entity;

import java.util.List;
import java.util.Map;

/**
 * @ClassName EsRespEntity
 * @Description TODO es 返回的数据的实体类
 * @Author zhouyang
 * @Date 2019/3/21 10:58
 * @Version 1.0
 **/
public class EsRespEntity {
    private int respCode;
    private String respMsg;
    private long totalCount;
    private List<Map<String, Object>> dataList;

    public int getRespCode() {
        return respCode;
    }

    public void setRespCode(int respCode) {
        this.respCode = respCode;
    }

    public String getRespMsg() {
        return respMsg;
    }

    public void setRespMsg(String respMsg) {
        this.respMsg = respMsg;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public List<Map<String, Object>> getDataList() {
        return dataList;
    }

    public void setDataList(List<Map<String, Object>> dataList) {
        this.dataList = dataList;
    }
}
