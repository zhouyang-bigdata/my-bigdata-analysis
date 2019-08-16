package com.bigdata.elasticsearch.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName ParseInitReqParamsUtils
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/4/25 16:31
 * @Version 1.0
 **/
public class ParseInitReqParamsUtils {

    /*
     * @Author zhouyang
     * @Description TODO 解析信号查询条件字符串
     * @Date 16:32 2019/4/25
     * @Param []
     * @return
     **/
    public static String parseInitReqParams(String signalOptionValue){
        if("".equals(signalOptionValue)){
            return "";
        }
        String[] signalOptionValueArr = signalOptionValue.split(";", -1);
        String reqSignalStr = null;
                //
        StringBuffer reqSignalStrb = new StringBuffer();
        reqSignalStrb.append("[");
        //
        for(int j=0;j<signalOptionValueArr.length;j++){
            String signalOptionValueJ = signalOptionValueArr[j];
            String[] signalValueJArr = signalOptionValueJ.split(",", -1);
            if(signalValueJArr.length<3){
                return null;
            }
            String signalNameJ = signalValueJArr[0];
            //
            String isRange = signalValueJArr[1];
            String equalValue = "";
            String minValue = "";
            String maxValue = "";
            //将信号条件转成格式：[{"p":"1 = 0"},{"p":"2 > 1"},{"p":"3 > 6,3 < 9"}]
            reqSignalStrb.append("{\"p\":\"");
            if("1".equals(isRange)){
                if(signalValueJArr.length<4){
                    return null;
                }
                //区间
                minValue = signalValueJArr[2];
                maxValue = signalValueJArr[3];
                reqSignalStrb.append(signalNameJ + " >= " + minValue);
                reqSignalStrb.append(",");
                reqSignalStrb.append(signalNameJ + " <= " + maxValue);
            }else{

                //等于值
                equalValue = signalValueJArr[2];
                reqSignalStrb.append(signalNameJ);
                reqSignalStrb.append(" = ");
                reqSignalStrb.append(equalValue);
            }

            reqSignalStrb.append("\"}");
            reqSignalStrb.append(",");

        }
        //减去最后一个逗号
        reqSignalStr = reqSignalStrb.substring(0, reqSignalStrb.length() - 1);
        reqSignalStr = reqSignalStr + "]";

        return reqSignalStr;
    }

    /*
     * @Author zhouyang
     * @Description TODO
     * @Date 9:47 2019/4/29
     * @Param []
     * @return
     **/
    public static String addSampleTimeToReqCondition(String sampleTimeName, Date startDate, Date endDate, String exportParamConditionListStr){
        //
        StringBuffer sampleTimeJsonStrb = new StringBuffer();
        sampleTimeJsonStrb.append("{\"p\":\"");
        sampleTimeJsonStrb.append(sampleTimeName + " >= " + startDate.getTime());
        sampleTimeJsonStrb.append(",");
        sampleTimeJsonStrb.append(sampleTimeName + " <= " + endDate.getTime());
        sampleTimeJsonStrb.append("\"}");
        if(!StringUtils.isEmpty(exportParamConditionListStr)){
            //去掉]
            exportParamConditionListStr = exportParamConditionListStr.substring(0, exportParamConditionListStr.length() -1);
            exportParamConditionListStr = exportParamConditionListStr + ",";
        }else{
            exportParamConditionListStr = "[" + exportParamConditionListStr;
        }
        //追加时间戳区间
        exportParamConditionListStr = exportParamConditionListStr + sampleTimeJsonStrb.toString();
        //加上]
        exportParamConditionListStr = exportParamConditionListStr + "]";
        return exportParamConditionListStr;
    }

    /*
     * @Author zhouyang
     * @Description TODO
     * @Date 14:35 2019/5/1
     * @Param []
     * @return
     **/
    public static String getGBRespParam(String signalValue){

        return null;
    }

    /*
     * @Author zhouyang
     * @Description TODO
     * @Date 14:35 2019/5/1
     * @Param []
     * @return
     **/
    public static String getQBRespParam(String signalValue){

        return null;
    }



//    public static void main(String[] args){
//        String signalValue = "INFO_TYPE_1_SIGNAL_1,INFO_TYPE_1_SIGNAL_4";
//        String signalOptionValue = "INFO_TYPE_1_SIGNAL_1,0,1;INFO_TYPE_1_SIGNAL_4,1,1,80";
//        String result = parseInitReqParams(signalOptionValue);
//        System.out.println(result);
//    }

    public static void main (String[] args){
        String s = "info_type_1_signal_1='3',info_type_1_signal_2='1',info_type_1_signal_3='2',info_type_1_signal_4>'1',info_type_1_signal_4<'1111',info_type_1_signal_5>'1',info_type_1_signal_5<'222222',info_type_1_signal_6>'1',info_type_1_signal_6<'12',info_type_1_signal_7>'1',info_type_1_signal_7<'12',info_type_1_signal_8>'1',info_type_1_signal_8<'43',info_type_1_signal_9='1',info_type_1_signal_10='GEARS_0101',info_type_1_signal_11>'1',info_type_1_signal_11<'5544'";
        Map<String, String> map = new HashMap<>();
        String[] split = s.split(",");
        for (String item : split){
            item = item.replaceAll("'", "");
            if(item.contains("=")){
                String[] split2 = item.split("=");
                String a = split2[0] + ",0," + split2[1];
                map.put(split2[0], a);
            }else if(item.contains(">")){
                String[] split2 = item.split(">");
                if(map.get(split2[0]) == null){
                    String a = split2[0] + ",1," + split2[1];
                    map.put(split2[0], a);
                }else {
                    String a = map.get(split2[0]);//key,
                    String[] b = a.split(",");
                    map.put(split2[0], b[0] + "," + b[1] + "," + split2[1] + "," + b[2]);

                }
            }else {
                String[] split2 = item.split("<");
                if(map.get(split2[0]) == null){
                    String a = split2[0] + ",1," + split2[1];
                    map.put(split2[0], a);
                }else {
                    String a = map.get(split2[0]);//key,
                    a = a + "," + split2[1];
                    map.put(split2[0], a);
                }
            }
        }
        String value = "";
        for (Map.Entry<String, String> item : map.entrySet()){
            value = value + item.getValue() + ";";
        }
        System.out.println(value.substring(0, value.length() - 1));

    }
}
