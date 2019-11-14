package com.globalegrow.tianchi.util;

import org.apache.commons.lang3.StringUtils;

public class SiteUtil {

    public static String getAppSite(String appName) {
        if (StringUtils.isNotEmpty(appName)) {
            String lowerName = appName.toLowerCase();
            if (lowerName.contains("zaful")) {
                return "ZF";
            }
            if (lowerName.contains("gearbest")) {
                return "GB";
            }
            if (lowerName.contains("rosegal")) {
                return "RG";
            }
        }
        return "";
    }

}
