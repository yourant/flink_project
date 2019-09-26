package com.globalegrow.tianchi.test.kafka;

import com.alibaba.fastjson.JSONObject;

public class JsonMocker {

    public JsonMocker() {

    }

    public static  String initEventLog(String startLogJson) {
        JSONObject jsonObject = new JSONObject();
        if ("af_add_to_wishlist".equals(startLogJson)) {
            jsonObject.put("event_name", "af_add_to_wishlist");
            jsonObject.put("bundle_id", "com.zaful.Zaful");
            jsonObject.put("app_version", "4.5.2");

            jsonObject.put("event_value", "{'af_content_type':'product','af_bucket_id':'0','af_plan_id':'873','af_country_code':'US','af_price':'14.04','af_changed_size_or_color':'0','af_inner_mediasource':'category_88','af_content_category':'/Women/Bottoms/Skirts','af_currency':'USD','af_content_id':'451400205','af_sort':'recommend','af_version_id':'2544','af_national_code':'ZF','af_bts':[{'planid':'896','bucketid':'98','policy':'C','plancode':'iosproductphoto','versionid':'2608'},{'planid':'873','bucketid':'0','policy':'C','plancode':'categoryios','versionid':'2544'},{'planid':'693','bucketid':'64','policy':'1','plancode':'ioscolour','versionid':'2028'},{'planid':'826','bucketid':'72','policy':'C','plancode':'iosdetail','versionid':'2423'}],'af_lang':'en'}");
            //jsonObject.put("event_value", "{'af_content_type':'product','af_bucket_id':'0','af_plan_id':'873','af_country_code':'US','af_price':'14.04','af_changed_size_or_color':'0','af_inner_mediasource':'category_88','af_content_category':'/Women/Bottoms/Skirts','af_currency':'USD','af_content_id':'451400205','af_sort':'recommend','af_version_id':'2544','af_national_code':'ZF','af_lang':'en'}");

            return jsonObject.toJSONString();

        }

        return null;
    }

}
