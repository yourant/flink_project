package com.globalegrow.tianchi.util;


import cn.hutool.http.HttpRequest;

import java.util.ArrayList;
import java.util.List;


/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 16:28 2019/11/1
 * @Modified:
 */
public class ESUtil {

    public static void main(String[] args) {

//        List<String> list = new ArrayList<>();
//
//        list.add("expose");
//        list.add("click");
//        list.add("adt");
//        list.add("collect");
//
//        list.add("search");
//        list.add("order");
//        list.add("purchase");
//
//        for (String index : list) {
//
//            ESUtil.httpCreateIndex("100.26.74.93", "9202",
//                    "zaful"+"_"+"impress"+"_"+index+"_event_realtime",3,1);
//        }

        ESUtil.httpCreateIndex("100.26.74.93", "9202",
                "zaful_pc_expose_count_realtime",3,1);
    }

    /**
     * 创建索引
     *
     * @param hostName
     * @param port
     * @param index
     * @param number_of_shards
     * @param number_of_replicas
     */
    public static void httpCreateIndex(String hostName, String port,
                                       String index, int number_of_shards, int number_of_replicas){
        try {
            String json = "{\"settings\": { \"index\": { \"number_of_shards\": \""
                    + number_of_shards + "\", \"number_of_replicas\": \"" + number_of_replicas + "\" }}}";

            HttpRequest.put("http://" + hostName + ":" + port + "/" + index).body(json).execute().body();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
