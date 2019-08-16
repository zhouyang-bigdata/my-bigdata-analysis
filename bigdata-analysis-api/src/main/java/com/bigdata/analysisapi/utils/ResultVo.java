package com.bigdata.analysisapi.utils;


import com.bigdata.analysisapi.constants.CommonErrorCodeEnum;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.ToString;

/**
 * @Description: java类作用描述
 * @Author: 邓忠情
 * @CreateDate: 2019/2/18 15:06
 * @Version: 1.0
 */
@Data
@ToString
public class ResultVo<T> {

    /**
     * 错误代码
     */
    private int code = CommonErrorCodeEnum.ERROR_SUCCESS.getCode();
    /**
     * 错误信息
     */
    private String message = "调用成功";

    @ApiModelProperty(value ="第三方业务编码")
    private int bizCode = CommonErrorCodeEnum.ERROR_SUCCESS.getCode();
    /**
     * 第三方错误信息
     */
    private String bizMessage = "调用成功";

    /**
     * 返回数据
     */
    private T data;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getBizCode() {
        return bizCode;
    }

    public void setBizCode(int bizCode) {
        this.bizCode = bizCode;
    }

    public String getBizMessage() {
        return bizMessage;
    }

    public void setBizMessage(String bizMessage) {
        this.bizMessage = bizMessage;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
