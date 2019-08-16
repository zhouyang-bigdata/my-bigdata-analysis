package com.bigdata.elasticsearch.entity;

import java.io.Serializable;

/**
 * @ClassName ReqParamConditionEntity
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/3/20 11:46
 * @Version 1.0
 **/
public class ReqParamConditionEntity implements Serializable {
    //原始数据的值
    private String initValue;
    //
    private String paramName;
    //
    private String paramValue;
    //
    private double minValue;
    //
    private double maxValue;
    //是否是小于
    private boolean isLt = false;
    //是否是大于
    private boolean isGt = false;
    //是否是小于等于
    private boolean isLte = false;
    //是否是大于等于
    private boolean isGte = false;
    //是否等于
    private boolean isEqual = false;

    public String getInitValue() {
        return initValue;
    }

    public void setInitValue(String initValue) {
        this.initValue = initValue;
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getParamValue() {
        return paramValue;
    }

    public void setParamValue(String paramValue) {
        this.paramValue = paramValue;
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public boolean isLte() {
        return isLte;
    }

    public void setLte(boolean lte) {
        isLte = lte;
    }

    public boolean isGte() {
        return isGte;
    }

    public void setGte(boolean gte) {
        isGte = gte;
    }

    public boolean isLt() {
        return isLt;
    }

    public void setLt(boolean lt) {
        isLt = lt;
    }

    public boolean isGt() {
        return isGt;
    }

    public void setGt(boolean gt) {
        isGt = gt;
    }

    public boolean isEqual() {
        return isEqual;
    }

    public void setEqual(boolean equal) {
        isEqual = equal;
    }

    @Override
    public String toString() {
        return "ReqParamConditionEntity{" +
                "initValue='" + initValue + '\'' +
                ", paramName='" + paramName + '\'' +
                ", paramValue='" + paramValue + '\'' +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                ", isLt=" + isLt +
                ", isGt=" + isGt +
                ", isLte=" + isLte +
                ", isGte=" + isGte +
                ", isEqual=" + isEqual +
                '}';
    }
}
