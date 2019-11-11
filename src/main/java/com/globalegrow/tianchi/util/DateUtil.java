package com.globalegrow.tianchi.util;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 18:29 2019/11/2
 * @Modified:
 */
public class DateUtil {


    /**
     * 13位时间戳字符串转成日期字符串
     * @param timeStamp 时间戳
     * @param format 日期格式：（"yyyy-MM-dd HH:mm:ss"，"yyyyMMddHH"）
     * @return
     */
    public static String timeStamp2DateStr(String timeStamp, String format){

        double time = Long.valueOf(timeStamp);
        String result = new SimpleDateFormat(format).format(time);

        return result;
    }

    public static Timestamp timeStamp2Date(String timeStamp){
        double time = Double.valueOf(timeStamp);
        String result = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
        Timestamp ts = Timestamp.valueOf(result);

        return ts;
    }

}
