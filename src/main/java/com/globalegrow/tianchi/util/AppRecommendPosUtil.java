package com.globalegrow.tianchi.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class AppRecommendPosUtil {

    public static String getAppRecommendPosName(String eventValue) {
        try {
            return getAppRecommendPosName(JacksonUtil.readValue(eventValue, Map.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "others";
    }

    public static String getAppRecommendPosName(Map<String, Object> valueMap) {
        if (valueMap.get("af_inner_mediasource") instanceof String) {
            String s = (String)valueMap.get("af_inner_mediasource");
            if (StringUtils.isEmpty(s)) {
                s = "others";
            } else if (s.indexOf("category_") >= 0) {
                s = "category";
            } else if (s.indexOf("search_") >= 0) {
                s = "search";
            } else if (s.indexOf("recommend_personnal_recentviewed") >= 0) {
                s = "recommend_personnal_recentviewed";
            } else if (s.indexOf("unknowmediasource") >= 0) {
                s = "unknowmediasource";
            } else if (s.indexOf("promotion_") >= 0) {
                s = "promotion";
            } else if (s.indexOf("recommend_collocation") >= 0) {
                s = "recommend_collocation";
            } else if (s.indexOf("recommend productdetail") >= 0) {
                s = "recommend productdetail";
            } else if (s.indexOf("recommend_push") >= 0) {
                s = "recommend_push ";
            } else if (s.indexOf("recommend_wishlist") >= 0) {
                s = "recommend_wishlist";
            } else if (s.indexOf("newusers_exclusive") >= 0) {
                s = "newusers_exclusive";
            } else if (s.indexOf("newusers_flashsale") >= 0) {
                s = "newusers_flashsale";
            }  else if (s.indexOf("recommend_zme_exploreid_") >= 0) {
                s = "recommend_zme_exploreid";
            } else if (s.indexOf("recommend_zme_outfitid_") >= 0) {
                s = "recommend_zme_outfitid";
            } else if (s.indexOf("virtual_category_") >= 0) {
                s = "virtual_category";
            } else if (s.indexOf("recommend_coudan") >= 0) {
                s = "recommend_coudan";
            } else if (s.indexOf("recommend_cartpage") >= 0) {
                s = "recommend_cartpage";
            } else if (s.indexOf("discount_product") >= 0) {
                s = "discount_product";
            } else if (s.indexOf("gift_product") >= 0) {
                s = "gift_product";
            } else if (s.indexOf("recommend_channel_") >= 0) {
                s = "recommend_channel";
            } else if (s.indexOf("recommend_homepage") >= 0) {
                s = "recommend_homepage";
            } else {
                s = "others";
            }
            return s;
        }
        return "others";
    }

}
