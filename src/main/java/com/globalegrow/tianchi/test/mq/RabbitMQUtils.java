package com.globalegrow.tianchi.test.mq;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * {"site":"FZ","status":3,"time":1541063818182,"date":'2018-11-01'}  //es开始时间
 * {"site":"FZ","status":4,"time":1541063818182,"date":'2018-11-01'}  //es结束时间
 *
 * 电子站 GB  服装站 FZ  (不分站点  site 为空)
 * 0 check决策系统数据是否到位
 * 1 spark计算开始
 * 2 spark计算结束
 * 3 写入es开始
 * 4 写入es结束
 * 5 抽数开始
 * 6 抽数结束
 */
public class RabbitMQUtils {
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    private static Gson gson = GsonUtil.getGson();

    public static void publishMsg(int msg) {
        EndInfo info = new EndInfo();
        initInfo(msg, info);
        Channel chan = null;
        if (info.getStatus() == 7) {
            System.out.println("Info state ERROR");
        }
        try {
            chan = getChannel();
            chan.basicPublish("", "labelUsers_AI_EMAIL", null, gson.toJson(info).getBytes());
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        } finally {
            try {
                if (chan != null) {
                    chan.getConnection().close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Channel getChannel() throws IOException, TimeoutException {
       // Properties prop = CommonUtils.getProperties("config.properties");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("conn.amazon.rabbitmq.hqygou.com");
        factory.setPort(5672);
        factory.setUsername("qrqm_product");
        factory.setPassword("BW0HutPr");
        factory.setVirtualHost("CRM");
        Connection connection = factory.newConnection();
        return connection.createChannel();
    }

    /**
     * 根据传入状态码初始化信息
     * 0-抽数完成
     * 1-计算开始
     * 2-计算结束
     * 31-服装写入ES开始
     * 41-服装写入ES结束
     * 32-GB写入ES开始
     * 42-GB写入ES结束
     *
     * @param msg  状态码
     * @param info 信息对象
     */
    private static void initInfo(int msg, EndInfo info) {
        info.setTime(System.currentTimeMillis());
        info.setDate(df.format(new Date()));
        switch (msg) {
            case 0:
                info.setSite("");
                info.setStatus(0);
                break;
            case 1:
                info.setSite("");
                info.setStatus(1);
                break;
            case 2:
                info.setSite("");
                info.setStatus(2);
                break;
            case 31:
                info.setSite("FZ");
                info.setStatus(3);
                break;
            case 41:
                info.setSite("FZ");
                info.setStatus(4);
                break;
            case 32:
                info.setSite("GB");
                info.setStatus(3);
                break;
            case 42:
                info.setSite("GB");
                info.setStatus(4);
                break;
            default:
                info.setSite("");
                info.setStatus(7);
                break;
        }
    }
}
