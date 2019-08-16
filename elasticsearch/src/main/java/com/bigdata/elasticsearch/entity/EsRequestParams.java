package com.bigdata.elasticsearch.entity;

import java.util.List;
import java.util.Map;

/**
 * @ClassName EsRequestParams
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/3/20 13:52
 * @Version 1.0
 **/
public class EsRequestParams {
    //要查询显示出来的参数map
    private Map<String, String> queryParams;
    //查询条件，过滤的参数条件map
    private Map<String, String> filterParams;
    //开始行
    private int startRow;
    //查询显示的条数
    private int showCount;

    //
    //查询条件List
    private List<ReqParamConditionEntity> reqParamConditionList;
    //返回的参数list
    private List<String> respParamList;

    public Map<String, String> getQueryParams() {
        return queryParams;
    }

    public void setQueryParams(Map<String, String> queryParams) {
        this.queryParams = queryParams;
    }

    public Map<String, String> getFilterParams() {
        return filterParams;
    }

    public void setFilterParams(Map<String, String> filterParams) {
        this.filterParams = filterParams;
    }

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public int getShowCount() {
        return showCount;
    }

    public void setShowCount(int showCount) {
        this.showCount = showCount;
    }

    public List<ReqParamConditionEntity> getReqParamConditionList() {
        return reqParamConditionList;
    }

    public void setReqParamConditionList(List<ReqParamConditionEntity> reqParamConditionList) {
        this.reqParamConditionList = reqParamConditionList;
    }

    public List<String> getRespParamList() {
        return respParamList;
    }

    public void setRespParamList(List<String> respParamList) {
        this.respParamList = respParamList;
    }
}
