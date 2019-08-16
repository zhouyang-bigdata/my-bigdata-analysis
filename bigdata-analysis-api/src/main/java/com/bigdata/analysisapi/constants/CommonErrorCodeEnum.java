package com.bigdata.analysisapi.constants;

/**
 * @描述:
 * @公司:sziov
 * @作者:cy
 * @版本: 1.0.0
 * @日期: 2019-01-29 14:02:12
 */
public enum CommonErrorCodeEnum {
    /**
     * 错误编号---登录验证码错误
     */
    ERROR_LOGINVALIDATECODEERROR(-100001, "登录验证码错误"),
    /**
     * 错误编号---登录失败，账号或密码错误
     */
    ERROR_LOGINERROR(-100002, "登录失败，账号或密码错误"),
    /**
     * 错误编号---登录失败，账号已过期请联系管理员
     */
    ERROR_EFFECTIVE_DATETIME(-100003, "登录失败，账号已过期请联系管理员"),
    /**
     * 错误编号---登录失败，账号已被禁用请联系管理员
     */
    ERROR_BEDISABLE(-100004, "登录失败，账号已被禁用请联系管理员"),
    /**
     * 非法微信用户
     */
    ERROR_ILLEGAL_USER(-100005, "当前登陆用户信息不存在"),
    /**
     * 微信黑名单用户
     */
    ERROR_BLACKLIST(-100009, "黑名单用户，请稍后再试"),
    /**
     * 第三方接口调用异常
     */
    ERROR_THIRD_INTERFACE_INVOKE(-100006, "第三方接口调用异常"),

    ERROR_REPEAT_ACCOUNT(-100007, "已存在角色!"),
    /**
     * 错误编号---签名错误
     */
    ERROR_SIGN(-100008, "签名错误"),
    /*
    "70001"+ "解析APK异常！ "
     */
    ERROR_APK_PARSE(-100010, "解析APK异常"),

    /*
    "-3109"+ "记录添加失败！ "
     */
    ERROR_ADD_RECORD(-100012, "记录添加失败!"),

    /**
     * 错误编号---删除失败
     */
    ERROR_DELETE(-100013, "删除失败"),
    ERROR_VIN_CODE_IS_BLANK(-100014, "vin为空"),
    /**
     * 错误编号---数据为空
     */
    ERROR_DATE_NULL(-110011, "数据为空"),

    /**
     * 错误编号---删除失败
     */
    ERROR_MODIFY(-110012, "修改失败"),
    /**
     * 错误编号---短信发送失败
     */
    ERROR_SMS_SEND(-110013, "下发短信失败"),
    /**
     * 错误编号---短信发送失败
     */
   // ERROR_SUCCESS(0, "调用成功");
    PARAMS_NULL_ERROR(-110014, "参数为空"),
    /**
     * 错误编号---签名错误
     */
    SINGTURE_ERROR(-110015, "签名错误"),
    /**
     * 系统时间戳失效
     */
    ERROR_SYSTEM_TIME(-110016, "系统时间戳失效"),
    /**
     * 错误编号---短信发送失败
     */
    ERROR_SUCCESS(0, "调用成功");

    private int code;

    private String message;

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

    private CommonErrorCodeEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }
}
