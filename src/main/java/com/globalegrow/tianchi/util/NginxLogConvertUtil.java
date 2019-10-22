package com.globalegrow.tianchi.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class NginxLogConvertUtil {


    public static final String PARAMETERS_PATTERN = "_ubc.gif\\??(.*)HTTP";
    public static final String TIMESTAMP_PATTERN = "\\^A\\^\\d{10}";

    public static final String BAD_JSON_PATTERN = ":([\\w]+_?)";

    public static final String TIMESTAMP_KEY = "timestamp";

    public static final String URL_PARAMETERS_PATTREN = "(.*?)=(.*?)&";
    // 用于处理 json 中存在未转义的 & 字符
    public static final String URL_PARAMETERS_JSON_PATTREN = "(.*?)=\\{(.*?)}&";

    public static final Pattern p = Pattern.compile(PARAMETERS_PATTERN);
    public static final Pattern timestamp = Pattern.compile(TIMESTAMP_PATTERN);
    public static final Pattern parameter = Pattern.compile(URL_PARAMETERS_PATTREN);
    public static final Pattern parameterJson = Pattern.compile(URL_PARAMETERS_JSON_PATTREN);

    public static final List<Pattern> parameters = new ArrayList(){

        private static final long serialVersionUID = -8623483750783717423L;
        {
            add(timestamp);
            add(parameter);
            add(parameterJson);
        }
    };

    //private static final

    public static String handleBadJson(String sourceValue) {
        String badJson = sourceValue;
        Pattern p = Pattern.compile(BAD_JSON_PATTERN);
        Matcher m = p.matcher(badJson);
        List<String> key = new ArrayList<>();
        while (m.find()) {
            String badName = m.group();
            key.add(badName);
        }
        for (String badName : key) {
            badJson = badJson.replaceAll(badName, "\"" + badName.replaceAll(":", "") + "\":");
        }
        return badJson;
    }

    /**
     * 将原始埋点数据转换为 map
     *
     * @param log
     * @return
     */
    public static Map<String, Object> getNginxLogParameters(String log) {
        Matcher m = p.matcher(log);
        String requestStr = "";

        while (m.find()) {
            requestStr = m.group();
        }

        return AppLogConvertUtil.getStringObjectMap(log, requestStr, TIMESTAMP_KEY);
    }

    public static Long getTimestamp(String log) {

        Matcher m = null;
        try {
            m = timestamp.matcher(URLDecoder.decode(log, "utf-8"));
        } catch (Exception e) {
            m = timestamp.matcher(log);
        }
        String requestStr = "";

        while (m.find()) {
            requestStr = m.group();
        }
        if (StringUtils.isNotEmpty(requestStr)) {
            String pattern = "\\d";
            Pattern p1 = Pattern.compile(pattern);
            Matcher m1 = p1.matcher(requestStr);
            StringBuilder time = new StringBuilder();
            while (m1.find()) {
                time.append(m1.group());
            }
            return Long.valueOf(time.toString()) * 1000;
        }

        return System.currentTimeMillis();
    }

    public static Map<String, Object> getUrlParams(String param) {

        Map<String, Object> map = new HashMap(0);
        if (StringUtils.isBlank(param)) {
            return map;
        }
        String[] params = param.split("&");
        for (int i = 0; i < params.length; i++) {
            String[] p = params[i].split("=");
            if (p.length == 2) {
                try {
                    String key = p[0];
                    String value = URLDecoder.decode(p[1].replaceAll("\t", "").replaceAll("\n", "").replaceAll("\r", "").replaceAll("\\\\x", "%"), "utf-8");
                    if (key.startsWith("_ubc.gif?")) {
                        map.put(key.replace("_ubc.gif?", ""), value);
                    } else if (key.startsWith("_app.gif?")) {
                        map.put(key.replace("_app.gif?", "").toLowerCase(), value);
                    } else {

                        map.put(p[0].toLowerCase(), valueHex(value));

                    }
                } catch (Exception e) {
                    map.put(p[0].toLowerCase(), valueHex(p[1].replaceAll("\t", "").replaceAll("\n", "").replaceAll("\r", "").replaceAll("\\\\x", "%")));
                }
            }
        }
        return map;
    }

    /**
     * {%22name%22:%22Ahorre%20hasta%20un%2060%%20de%20descuento%20|%20Los%20suministros%20de%20salud%20y%20belleza%22,%22type%22:%22gbsite_custom%22}
     * {"mrlc":"T_4","sku":"284401303","rank":2,"sckw":"Robes d'\xE9t\xE9","sk":"D","fmd":"mp"}
     *
     * @param o
     * @return
     */
    public static Object valueHex(Object o) {
        String s = String.valueOf(o);
        if (StringUtils.isNotEmpty(s)) {
            /*if ((s.contains("\\x22") || s.contains("\\\\x22"))) {
                s = s.replaceAll("\\\\x22", "\"").replaceAll("\\x22", "\"");
            }
            if ((s.contains("\\x5C") || s.contains("\\\\x22"))) {
                s = s.replaceAll("x5C", "");
            }
            if (StringUtils.isNotEmpty(s) && (s.contains("\\xC3") || s.contains("\\\\x22"))) {
                s = s.replaceAll("xC3", "");
            }
            if (s.contains("\\xE9t")) {
                s = s.replaceAll("xE9t", "");
            }
            if (s.contains("{'at'")) {
                s = s.replaceAll("\\{'at'", "{\"at\"");
            }*/
            /*if (s.indexOf("\\x22") > 0 || s.indexOf("\\x") > 0) {
                s = MyLogStringUtils.unescape_perl_string(s);
            }*/
            if (s.contains("%22")) {
                s = s.replaceAll("%22", "\"");
            }
            if (s.contains("{'at'")) {
                s = s.replaceAll("\\{'at'", "{\"at\"");
            }
            if (s.contains("{:") || s.contains("{\":")) {
                s = handleBadJson(s);
            }
        }
        return s;
    }
}
