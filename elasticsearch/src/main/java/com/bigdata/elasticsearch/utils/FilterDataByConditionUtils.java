package com.bigdata.elasticsearch.utils;


import com.bigdata.elasticsearch.entity.ReqParamConditionEntity;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FilterDataByConditionUtils
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/3/28 15:49
 * @Version 1.0
 **/
public class FilterDataByConditionUtils implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(FilterDataByConditionUtils.class);

    /*
     * @Author zhouyang
     * @Description TODO TODO 根据参数筛选条件，判断每一行数据是否符合，如果符合，返回true，否则，false
     * @Date 15:42 2019/4/9
     * @Param [reqParamConditionList, lineMap]
     * @return
     **/
    public static boolean filterParamsByReqCondition2(List<ReqParamConditionEntity> reqParamConditionList, Map<String, Object> lineMap){
        if(lineMap == null){
            return false;
        }
        //如果该条数据，被识别为“有命令标识：数据没有处理，其应答标识为：，可能是某个指令的结果包！”，返回false
        if(lineMap.size()<2){
            return false;
        }
        //遍历请求条件,在每一行的lineMap中，找出匹配的参数，并把原始数据中对应参数的值set 进去
        for (ReqParamConditionEntity reqParamConditionEntity : reqParamConditionList) {
            //列名
            String name = reqParamConditionEntity.getParamName();
            for (Map.Entry<String, Object> entry : lineMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if(name.equals(key)){
                    //有多少个参数筛选条件，就set 多少个参数的原始数据的值
                    reqParamConditionEntity.setInitValue(String.valueOf(value));
                }
            }
        }
        //这一行数据是否符合筛选条件
        boolean lineIsMatch = true;
        //遍历请求条件，有多少个参数筛选条件，就有多少个参数的原始数据的值
        for (ReqParamConditionEntity reqParamConditionEntity : reqParamConditionList) {
            //这个参数的值是否符合筛选条件
            boolean isMatch = false;
            //原始数据的值
            String initValue = reqParamConditionEntity.getInitValue();
            //列名
            String name = reqParamConditionEntity.getParamName();
            //列的值
            String value = reqParamConditionEntity.getParamValue();
            //列大于的值
            double minValue = reqParamConditionEntity.getMinValue();
            //列小于的值
            double maxValue = reqParamConditionEntity.getMaxValue();
            //是否是大于等于
            boolean isGte = reqParamConditionEntity.isGte();
            //是否是小于等于
            boolean isLte = reqParamConditionEntity.isLte();
            //是否是大于
            boolean isGt = reqParamConditionEntity.isGt();
            //是否是小于
            boolean isLt = reqParamConditionEntity.isLt();
            //是否是等于
            boolean isEqual = reqParamConditionEntity.isEqual();

            //如果某个查询字段有值，则matchQueryFlag = false
            //参数名为空，跳过
            if(StringUtils.isEmpty(name)){
                String msg = "参数名错误";
                LOG.error(msg);
                return false;
            }
            //只有等于
            if(isEqual){
                if(initValue.equals(value)){
                    isMatch = true;
                }
            }
            else{
                double initValueToLong = 0D;
                try{
                    initValueToLong = Double.valueOf(initValue).doubleValue();
                }catch (Exception e){
                    LOG.error("filterParamsByReqCondition error:", e);
                    return false;
                }
                //a<x<b 或 a<=x<b 或a<x<=b 或a<=x<=b
                if(isLt&&isGt){
                    if(!isLte&&!isGte){
                        //a<x<b
                        if((minValue<initValueToLong)&&(initValueToLong<maxValue)){
                            isMatch = true;
                        }
                    }else if(isLte&&!isGte){
                        //a<x<=b
                        if((minValue<initValueToLong)&&(initValueToLong<=maxValue)){
                            isMatch = true;
                        }
                    }else if(!isLte&&isGte){
                        //a<=x<b
                        if((minValue<=initValueToLong)&&(initValueToLong<maxValue)){
                            isMatch = true;
                        }
                    }else if(isLte&&isGte){
                        //a<=x<=b
                        if((minValue<=initValueToLong)&&(initValueToLong<=maxValue)){
                            isMatch = true;
                        }
                    }

                }else{
                    if(isLt){
                        if(!isLte){
                            //仅小于
                            if(initValueToLong<maxValue){
                                isMatch = true;
                            }
                        }else{
                            //小于等于
                            if(initValueToLong<=maxValue){
                                isMatch = true;
                            }
                        }
                    }else if(isGt){
                        if(!isGte){
                            //仅大于
                            if(initValueToLong>minValue){
                                isMatch = true;
                            }
                        }else{
                            //大于等于
                            if(initValueToLong>=minValue){
                                isMatch = true;
                            }
                        }
                    }
                }
            }

            //如果该参数的值是不符合筛选条件，跳出
            if(!isMatch){
                lineIsMatch = false;
                break;
            }
        }
        return lineIsMatch;
    }

    /*
     * @Author zhouyang
     * @Description TODO 根据要显示的字段，返回一个map
     * @Date 15:55 2019/4/9
     * @Param [lineMap, respParamList]
     * @return
     **/
    public static Map<String, Object> getDataByNeedRespParams(Map<String, Object> lineMap, List<String> respParamList){
        if(lineMap == null){
            return null;
        }
        Map<String, Object> resultMap = new HashMap<>();
        for(int i=0;i<respParamList.size();i++){
            String needParamName = respParamList.get(i);
            for (Map.Entry<String, Object> entry : lineMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if(needParamName.equals(key)){
                    resultMap.put(needParamName, value);
                    break;
                }
            }

        }

        return resultMap;
    }

    /*
     * @Author zhouyang
     * @Description TODO 根据要显示的字段，返回一个list
     * @Date 15:55 2019/4/9
     * @Param [lineMap, respParamList]
     * @return
     **/
    public static List<Object> getListByNeedRespParams(Map<String, Object> lineMap, List<String> respParamList){
        if(lineMap == null){
            return null;
        }
        List<Object> resultList = new ArrayList<>();
        for(int i=0;i<respParamList.size();i++){
            String needParamName = respParamList.get(i);
            for (Map.Entry<String, Object> entry : lineMap.entrySet()) {
                String key = entry.getKey();
                if(needParamName.equals(key)){
                    resultList.add(entry.getValue());
                    break;
                }
            }

        }

        return resultList;
    }


}
