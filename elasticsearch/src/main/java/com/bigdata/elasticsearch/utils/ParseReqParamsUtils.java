package com.bigdata.elasticsearch.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.elasticsearch.entity.ReqParamConditionEntity;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ParseReqParamsUtils
 * @Description TODO 解析前端请求参数json字符串工具类
 * @Author zhouyang
 * @Date 2019/3/21 16:44
 * @Version 1.0
 **/
public class ParseReqParamsUtils {
    private static Logger log = LoggerFactory.getLogger(ParseReqParamsUtils.class);
    /*
     * @Author zhouyang
     * @Description TODO 解析前端请求参数json字符串,转成list
     * @Date 16:47 2019/3/21
     * @Param []
     * @return 
     **/
    public static List<ReqParamConditionEntity> parseReqParams(String str){
        List<ReqParamConditionEntity> list = new ArrayList<>();
        if(StringUtils.isEmpty(str)){
            return list;
        }

        boolean isConditionError = false;
        JSONArray jsonArray = null;
        try{
            jsonArray = JSONArray.parseArray(str);
        }catch(Exception e){
            String errInfo = "解析前端请求参数json字符串错误，格式异常";
            log.error(errInfo, e);
        }
        //判断符号
        String equalSymbol = "=";
        String ltSymbol = "<";
        String gtSymbol = ">";

        //遍历，解析请求参数条件json
        for(Object o : jsonArray){
            JSONObject jsonObject = (JSONObject)o;
            String paramCondition = "" + jsonObject.get("p");
            //以逗号区分
            String[] conditionArr = paramCondition.split(",");


            //
            String pName = null;
            String pValue = null;
            double pMinValue = 0;
            double pMaxValue = 0;
            boolean pIsLt = false;
            boolean pIsGt = false;
            boolean pIsLte = false;
            boolean pIsGte = false;
            boolean pIsEqual = false;
            //
            double pMinValue2 = 0;
            double pMaxValue2 = 0;
            boolean pIsLt2 = false;
            boolean pIsGt2 = false;
            boolean pIsLte2 = false;
            boolean pIsGte2 = false;
            //
            String reqName = null;
            String reqValue = null;
            double reqMinValue = 0;
            double reqMaxValue = 0;
            boolean reqIsLt = false;
            boolean reqIsGt = false;
            boolean reqIsLte = false;
            boolean reqIsGte = false;
            boolean reqIsEqual = false;
            //第一个判断，false，小于；true，大于
            boolean firstJudge = false;
            //第二个判断，false，小于；true，大于
            boolean secondJudge = false;
            //
            ReqParamConditionEntity reqParamConditionEntity = new ReqParamConditionEntity();

            //
            if((conditionArr.length<1)||(conditionArr.length>2)){
                isConditionError = false;
                log.error("conditionArr.length error");
                break;
            }
            //
            for(int i=0;i<conditionArr.length;i++){
                //以空格区分
                String[] conditionChildArr1 = conditionArr[i].split(" ");
                if(conditionChildArr1.length<3){
                    isConditionError = true;
                    log.error("conditionChildArr1.length error");
                    break;
                }
                String name = conditionChildArr1[0];
                //
                String judgeSymbolStr = conditionChildArr1[1];
                //
                String value = conditionChildArr1[2];
                //
                boolean isEqual = false;
                boolean isLt = false;
                boolean isGt = false;
                //小于等于，大于等于
                boolean isLte = false;
                boolean isGte = false;
                if(judgeSymbolStr.indexOf(equalSymbol)!=-1) {
                    isEqual = true;
                }
                if(judgeSymbolStr.indexOf(ltSymbol)!=-1) {
                    isLt = true;
                }
                if(judgeSymbolStr.indexOf(gtSymbol)!=-1) {
                    isGt = true;
                }
                if(isLt&&isEqual){
                    isLte = true;
                }
                if(isGt&&isEqual){
                    isGte = true;
                }
                //第一个判断
                if(i==0){
                    pName = name;
                    pValue = value;
                    //
                    pIsLt = isLt;
                    pIsGt = isGt;
                    pIsLte = isLte;
                    pIsGte = isGte;
                    pIsEqual = isEqual;
                    //如果小于
                    if(pIsLt||pIsLte){
                        pMaxValue = Double.valueOf(value).doubleValue();
                    }
                    //如果大于
                    if(pIsGt||pIsGte){
                        pMinValue = Double.valueOf(value).doubleValue();
                        //第一个判断，false，小于；true，大于
                        firstJudge = true;
                    }
                }
                //第二个判断
                if(i==1) {
                    pName = name;
                    //
                    pIsLt2 = isLt;
                    pIsGt2 = isGt;
                    pIsLte2 = isLte;
                    pIsGte2 = isGte;
                    //如果小于
                    if (pIsLt2 || pIsLte2) {
                        pMaxValue2 = Double.valueOf(value).doubleValue();
                    }
                    //如果大于
                    if (pIsGt2 || pIsGte2) {
                        pMinValue2 = Double.valueOf(value).doubleValue();
                        //第二个判断，false，小于；true，大于
                        secondJudge = true;
                    }
                }
            }

            //
            if((conditionArr.length==2)&&!pIsEqual&&((pIsLt == pIsLt2)||(pIsGt == pIsGt2))){
                isConditionError = true;
                log.error("有2个< 号或有2个>号 error，错误码1");
                break;
            }
            //仅小于
            if(pIsLt||pIsLt2){
                //小于成立
                reqIsLt = true;
            }
            //仅大于
            if(pIsGt||pIsGt2){
                //大于成立
                reqIsGt = true;
            }
            //小于等于，成立
            if(pIsLte||pIsLte2){
                reqIsLte = true;
            }
            //大于等于，成立
            if(pIsGte||pIsGte2){
                reqIsGte = true;
            }
            //一个条件
            if(conditionArr.length==1){
                if(pIsEqual&&!reqIsGte&&!reqIsLte&&!reqIsLt&&!reqIsGt){
                    //等于
                    //赋值
                    reqIsEqual = pIsEqual;
                    reqValue = pValue;
                }else if(!pIsEqual&&(reqIsLt||reqIsGt)){
                    //小于，或大于，或小于等于，或大于等于
                    //赋值
                    reqMinValue = pMinValue;
                    reqMaxValue = pMaxValue;
                }
            }
            else{
                //2个条件
                if(conditionArr.length==2){
                    if((firstJudge&&secondJudge)||(!firstJudge&&!secondJudge)){
                        isConditionError = true;
                        log.error("有2个< 号或有2个>号 error，错误码2");
                        break;
                    }
                    //第一个大于，第二个小于
                    if(firstJudge&&!secondJudge){
                        reqMinValue = pMinValue;
                        reqMaxValue = pMaxValue2;
                    }
                    //第一个小于，第二个大于
                    if(!firstJudge&&secondJudge){
                        reqMinValue = pMinValue2;
                        reqMaxValue = pMaxValue;
                    }
                }

            }

            //
            reqName = pName;
            //将条件属性set 进实体类
            reqParamConditionEntity.setParamName(reqName);
            reqParamConditionEntity.setParamValue(reqValue);
            reqParamConditionEntity.setMinValue(reqMinValue);
            reqParamConditionEntity.setMaxValue(reqMaxValue);
            reqParamConditionEntity.setLt(reqIsLt);
            reqParamConditionEntity.setLte(reqIsLte);
            reqParamConditionEntity.setGt(reqIsGt);
            reqParamConditionEntity.setGte(reqIsGte);
            reqParamConditionEntity.setEqual(reqIsEqual);
            //
            list.add(reqParamConditionEntity);
        }

        if(isConditionError){
            log.error("解析前端请求参数json字符串:isConditionError true");
            List<ReqParamConditionEntity> emptyList = new ArrayList<>();
            return emptyList;
        }
        return list;
    }

    public static void main(String[] args){
        String str = "[\n" +
                "    {\n" +
                "        \"p\": \"info_type_8_signal_1 > 80,info_type_8_signal_1 <= 90\"\n" +
                "    }, \n" +
                "    {\n" +
                "        \"p\": \"info_type_8_signal_2 = 100\"\n" +
                "    }\n" +
                "    {\n" +
                "        \"p\": \"info_type_8_signal_3 < 120\"\n" +
                "    }\n" +
                "]";
        String str2 = "[{\"p\":\"BCS_YawRate = -0.00055\"},{\"p\":\"sampleTime >= 1554091200000,sampleTime <= 1556164800000\"}]";
        List<ReqParamConditionEntity> list = parseReqParams(str2);
        for(ReqParamConditionEntity r : list){
            System.out.println(r.toString());
        }
    }
}
