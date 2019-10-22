package com.globalegrow.tianchi.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class AppLogConvertUtil {

    public static final String PARAMETERS_PATTERN = "_app.gif\\??(.*)HTTP";
    //public static final String TIMESTAMP_PATTERN = "\\^A\\^\\d{10}";

    public static final String BAD_CONTENT_TYPE_22_22_W_W_22_PATTERN_STRING
        = "%22af_content_type%22:%22[\\w\\W&\\/]+?%22";
    public static final String BAD_CONTENT_CATEGORY_22_22_W_W_22_PATTERN_STRING
        = "%22af_content_category%22:%22[\\w\\W&\\/]+?%22";
    public static final Pattern BAD_CONTENT_TYPE_PATTERN = Pattern.compile(
        BAD_CONTENT_TYPE_22_22_W_W_22_PATTERN_STRING);
    public static final Pattern BAD_CONTENT_CATEGORY_PATTERN = Pattern.compile(
        BAD_CONTENT_CATEGORY_22_22_W_W_22_PATTERN_STRING);

    public static final String TIMESTAMP_KEY = "timestamp";

    /**
     * 将原始埋点数据转换为 map
     *
     * @param log
     * @return
     */
    public static Map<String, Object> getAppLogParameters(String log) {
            Pattern p = Pattern.compile(PARAMETERS_PATTERN);
            Matcher m = null;
            try {
                m = p.matcher(log);
            } catch (Exception e) {
                m = p.matcher(log);
            }
            String requestStr = "";

            while (m.find()) {
                requestStr = m.group();
            }

            while (m.find()) {
                requestStr = m.group();
            }

            Matcher sckwMatcher = BAD_CONTENT_TYPE_PATTERN.matcher(requestStr);

            String sckwStr = "";

            while (sckwMatcher.find()) {

                sckwStr = sckwMatcher.group();

            }

            if (StringUtils.isNotEmpty(sckwStr) && sckwStr.contains("&") && !sckwStr.contains("=")) {

                requestStr = requestStr.replaceAll(sckwStr, sckwStr.replaceAll("&", "%26"));

            }

            Matcher categoryMatcher = BAD_CONTENT_CATEGORY_PATTERN.matcher(requestStr);

            String categoryStr = "";

            while (categoryMatcher.find()) {

                categoryStr = categoryMatcher.group();

            }

            if (StringUtils.isNotEmpty(categoryStr) && categoryStr.contains("&") && !categoryStr.contains("=")) {

                requestStr = requestStr.replaceAll(categoryStr, categoryStr.replaceAll("&", "%26"));

            }

            Map<String, Object> result = getStringObjectMap(log, requestStr, TIMESTAMP_KEY);
            if (result != null) {
                return result;
            }

        return null;
    }

    static Map<String, Object> getStringObjectMap(String log, String requestStr, String timestampKey) {
        if (StringUtils.isNotEmpty(requestStr)) {
            if (requestStr.endsWith(" HTTP")) {
                requestStr = requestStr.substring(0, requestStr.lastIndexOf(" HTTP"));
            }

            Map<String, Object> result = NginxLogConvertUtil.getUrlParams(
                requestStr.replaceAll("%20&%20", " %26 ").replaceAll(" & ", " %26 "));
            if (result.size() > 0) {
                result.put(timestampKey, NginxLogConvertUtil.getTimestamp(log));
                return result;
            }

            return result;
        }
        return null;
    }

}
