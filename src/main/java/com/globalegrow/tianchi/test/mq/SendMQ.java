package com.globalegrow.tianchi.test.mq;

/**
 * <p>description</p>
 * Author: Ding Jian
 * Date: 2019-11-16 18:16:31
 */
public class SendMQ {

    public static void main(String[] args) {
        RabbitMQUtils.publishMsg(41);
    }
}
