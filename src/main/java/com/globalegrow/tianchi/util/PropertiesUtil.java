package com.globalegrow.tianchi.util;

import org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * @author zhougenggeng createTime  2019/9/26
 */
public class PropertiesUtil {

    /**
     * 读取hdfs上的配置文件
     *
     * @param fileName
     * @return
     */
    public static Properties getProperties(String fileName) {
        Properties prop = new Properties();
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(fileName);
            prop.load(inputStream);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;

    }

    /**
     * 读取本地上的配置文件
     *
     * @param fileName
     * @return
     */
    public static Properties loadProperties(String fileName) {
        Properties prop = new Properties();
        try {
            prop.load(
                new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName),
                    "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

}
