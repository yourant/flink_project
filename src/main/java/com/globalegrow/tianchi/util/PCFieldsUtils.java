package com.globalegrow.tianchi.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.AmountModel;
import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.bean.SkuInfo;
import com.globalegrow.tianchi.bean.SubEventField;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 19:00 2019/10/31
 * @Modified:
 */
public class PCFieldsUtils {

    public static List<String> getSkuFromSubEventFiled(Object subEventField){

        List<String> skuList = new ArrayList<>();

        if ((String.valueOf(subEventField)).startsWith("[")) {

            net.sf.json.JSONArray jsonArray = net.sf.json.JSONArray.fromObject(subEventField);

            List<SubEventField> list = new ArrayList<>();

            list = (List<SubEventField>) net.sf.json.JSONArray.toCollection(jsonArray, SubEventField.class);

            try {
                for (SubEventField subEventField1 : list) {
                    if (StringUtils.isBlank(subEventField1.getSku()) || null == subEventField1.getSku()) {
                        continue;
                    }

                    skuList.add(subEventField1.getSku());
                }
            }catch (Exception e){
                System.out.println("解析错误："+e);
            }

        }else {
            if ((String.valueOf(subEventField)).startsWith("{")) {

                try {
                    HashMap<String, Object> dataMap =
                            JSON.parseObject(String.valueOf(subEventField), new TypeReference<HashMap<String, Object>>() {
                            });


                    if ( dataMap.get("sku") != null) {
                        skuList.add((String) dataMap.get("sku"));
//                        System.out.println(dataMap.get("sku"));
                    }
                } catch (Exception e) {
                    System.err.println("json解析错误--->" + subEventField);

                }
            }
        }

        return skuList;
    }

    public static List<String> getSkuFromSkuInfo(Object skuInfo){

        List<String> skuList = new ArrayList<>();

            if ((String.valueOf(skuInfo)).startsWith("[")) {

                net.sf.json.JSONArray jsonArray = net.sf.json.JSONArray.fromObject(skuInfo);

                List<SkuInfo> list = new ArrayList<>();

                list = (List<SkuInfo>) net.sf.json.JSONArray.toCollection(jsonArray, SkuInfo.class);

                for (SkuInfo subEventField1 : list) {
                    if (StringUtils.isBlank(subEventField1.getSku()) || null==subEventField1.getSku()) {
                        continue;
                    }
                    skuList.add(subEventField1.getSku());
                }
            }else {
                if ((String.valueOf(skuInfo)).startsWith("{")) {

                    try {
                        HashMap<String, Object> dataMap =
                                JSON.parseObject(String.valueOf(skuInfo), new TypeReference<HashMap<String, Object>>() {
                                });

                        if ( dataMap.get("sku") != null) {
                            skuList.add((String) dataMap.get("sku"));
                        }
                    } catch (Exception e) {
                        System.err.println("json解析错误--->" + skuInfo);

                    }
                }
            }

        return skuList;
    }


    public static List<AmountModel> getSkuAmountFromSkuInfo(Object skuInfo){

        List<AmountModel> skuList = new ArrayList<>();

        if ((String.valueOf(skuInfo)).startsWith("[")) {

            net.sf.json.JSONArray jsonArray = net.sf.json.JSONArray.fromObject(skuInfo);

            List<SkuInfo> list = new ArrayList<>();

            list = (List<SkuInfo>) net.sf.json.JSONArray.toCollection(jsonArray, SkuInfo.class);

            for (SkuInfo subEventField1 : list) {
                if (StringUtils.isBlank(subEventField1.getSku()) || null==subEventField1.getSku()) {
                    continue;
                }

                AmountModel amountModel = new AmountModel();

                try {

                    amountModel.setSku(subEventField1.getSku());
                    amountModel.setPrice(Double.valueOf(subEventField1.getPrice()));
                    amountModel.setPam(Integer.valueOf(subEventField1.getPam()));
                }catch (Exception e){

                }

                skuList.add(amountModel);
            }
        }else {
            if ((String.valueOf(skuInfo)).startsWith("{")) {

                try {
                    HashMap<String, Object> dataMap =
                            JSON.parseObject(String.valueOf(skuInfo), new TypeReference<HashMap<String, Object>>() {
                            });

                    if ( dataMap.get("sku") != null) {
                        AmountModel amountModel = new AmountModel();

                        amountModel.setSku(String.valueOf(dataMap.get("sku")));
                        amountModel.setPrice(Double.valueOf(String.valueOf(dataMap.get("price"))));
                        amountModel.setPam(Integer.valueOf(String.valueOf(dataMap.get("pam"))));

                        skuList.add(amountModel);
                    }
                } catch (Exception e) {
                    System.err.println("json解析错误--->" + skuInfo);

                }
            }
        }

        return skuList;
    }



    public static Object getFiledsFromJson(String json, String filed){
        HashMap<String,Object> dataMap =
                JSON.parseObject(json,new TypeReference<HashMap<String,Object>>() {});

        return dataMap.get(filed);
    }



    public static boolean isZafulCodeSite(String json){
        HashMap<String,Object> dataMap =
                JSON.parseObject(json,new TypeReference<HashMap<String,Object>>() {});

        boolean isZaful = true;

        if (!((String)dataMap.get("site_code")).equals("10013")){
            isZaful = false;
        }

        return isZaful;
    }


    public static PCLogModel getPCLogModel(String value){

        HashMap<String,Object> dataMap =
                JSON.parseObject(value,new TypeReference<HashMap<String,Object>>() {});

        PCLogModel pcLogModel = new PCLogModel();

        pcLogModel.setSent_bytes_size(String.valueOf(dataMap.get("sent_bytes_size")));
        pcLogModel.setSearch_input_word(String.valueOf(dataMap.get("search_input_word")));
        pcLogModel.setOsr_referer_url(String.valueOf(dataMap.get("osr_referer_url")));
        pcLogModel.setBehaviour_type(String.valueOf(dataMap.get("behaviour_type")));
        pcLogModel.setUrl_suffix(String.valueOf(dataMap.get("url_suffix")));
        pcLogModel.setSkuinfo(String.valueOf(dataMap.get("skuinfo")));
        pcLogModel.setBts(String.valueOf(dataMap.get("bts")));
        pcLogModel.setSearch_suk_type(String.valueOf(dataMap.get("search_suk_type")));
        pcLogModel.setUnix_time(String.valueOf(dataMap.get("unix_time")));
        pcLogModel.setCountry_number(String.valueOf(dataMap.get("country_number")));
        pcLogModel.setFingerprint(String.valueOf(dataMap.get("fingerprint")));
        pcLogModel.setTime_stamp(String.valueOf(dataMap.get("time_stamp")));
        pcLogModel.setPage_info(String.valueOf(dataMap.get("page_info")));
        pcLogModel.setSession_id(String.valueOf(dataMap.get("session_id")));
        pcLogModel.setUser_ip(String.valueOf(dataMap.get("user_ip")));
        pcLogModel.setSearch_type(String.valueOf(dataMap.get("search_type")));
        pcLogModel.setTime_local(String.valueOf(dataMap.get("time_local")));
        pcLogModel.setOsr_landing_url(String.valueOf(dataMap.get("osr_landing_url")));
        pcLogModel.setClick_times(String.valueOf(dataMap.get("click_times")));
        pcLogModel.setLogin_status(String.valueOf(dataMap.get("login_status")));
        pcLogModel.setSite_code(String.valueOf(dataMap.get("site_code")));
        pcLogModel.setReal_client_ip(String.valueOf(dataMap.get("real_client_ip")));
        pcLogModel.setAmp(String.valueOf(dataMap.get("amp")));
        pcLogModel.setSub_event_info(String.valueOf(dataMap.get("sub_event_info")));
        pcLogModel.setAccept_language(String.valueOf(dataMap.get("accept_language")));
        pcLogModel.setUser_agent(String.valueOf(dataMap.get("user_agent")));
        pcLogModel.setUser_id(String.valueOf(dataMap.get("user_id")));
        pcLogModel.setLast_page_url(String.valueOf(dataMap.get("last_page_url")));
        pcLogModel.setCountry_code(String.valueOf(dataMap.get("country_code")));
        pcLogModel.setPage_main_type(String.valueOf(dataMap.get("page_main_type")));
        pcLogModel.setLanguage(String.valueOf(dataMap.get("language")));
        pcLogModel.setSearch_result_word(String.valueOf(dataMap.get("search_result_word")));
        pcLogModel.setEvent_type(String.valueOf(dataMap.get("event_type")));
        pcLogModel.setSub_event_field(String.valueOf(dataMap.get("sub_event_field")));
        pcLogModel.setSku_warehouse_info(String.valueOf(dataMap.get("sku_warehouse_info")));
        pcLogModel.setPage_module(String.valueOf(dataMap.get("page_module")));
        pcLogModel.setPage_code(String.valueOf(dataMap.get("page_code")));
        pcLogModel.setPage_sub_type(String.valueOf(dataMap.get("page_sub_type")));
        pcLogModel.setReferer(String.valueOf(dataMap.get("referer")));
        pcLogModel.setLink_id(String.valueOf(dataMap.get("link_id")));
        pcLogModel.setActivity_template(String.valueOf(dataMap.get("activity_template")));
        pcLogModel.setPlatform(String.valueOf(dataMap.get("platform")));
        pcLogModel.setUser_name(String.valueOf(dataMap.get("user_name")));
        pcLogModel.setSearch_click_position(String.valueOf(dataMap.get("search_click_position")));
        pcLogModel.setCookie_id(String.valueOf(dataMap.get("cookie_id")));
        pcLogModel.setOther(String.valueOf(dataMap.get("other")));
        pcLogModel.setCurrent_page_url(String.valueOf(dataMap.get("current_page_url")));
        pcLogModel.setCountry_name(String.valueOf(dataMap.get("country_name")));
        pcLogModel.setPage_stay_time(String.valueOf(dataMap.get("page_stay_time")));
        pcLogModel.setLog_id(String.valueOf(dataMap.get("log_id")));

//        Gson gson = new Gson();
//        GsonParseMoGuBean mogujie = gson.fromJson(jsonData, GsonParseMoGuBean.class);

        return pcLogModel;
    }


}
