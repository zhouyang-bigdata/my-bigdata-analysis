package com.bigdata.analysisapi.utils;


import com.bigdata.analysisapi.constants.CommonErrorCodeEnum;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 17:56 2019/7/11
 * @Param
 * @return
 **/
public class ResultUtils {

    /**
     * @描述: 调用成功，返回数据
     * @作者: 刘恺
     * @时间: 2017年11月16日 下午5:44:47
     * @param data
     * @return
     */
    public static <T> ResultVo<T> success(T data) {
        ResultVo<T> result = new ResultVo<T>();
        result.setCode(CommonErrorCodeEnum.ERROR_SUCCESS.getCode());
        result.setMessage(CommonErrorCodeEnum.ERROR_SUCCESS.getMessage());
        result.setData(data);
        return result;
    }

    /**
     * @描述: 返回错误响应
     * @作者: 刘恺
     * @时间: 2017年11月16日 下午5:44:55
     * @param code
     * @param msg
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static ResultVo error(int code, String msg) {
        ResultVo result = new ResultVo();
        result.setCode(code);
        result.setMessage(msg);
        result.setData(null);
        return result;
    }

    /**
     *
     * @param code
     *
     * @param msg
     * @param bizCode 第三方错误CODE
     * @param bizMsg  第三方错误信息
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static ResultVo error(int code, String msg, int bizCode, String bizMsg) {
        ResultVo result = new ResultVo();
        result.setCode(code);
        result.setBizCode(bizCode);
        result.setMessage(msg);
        result.setData(null);
        return result;
    }
    /**
     * @描述: 调用成功，无数据返回
     * @作者: 刘恺
     * @时间: 2017年11月16日 下午5:45:02
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static ResultVo success() {
        return success(null);
    }
}
