package com.globalegrow.tianchi.bean;

import lombok.Data;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 0:33 2019/11/11
 * @Modified:
 */
@Data
public class ZafulPcFeedBackCount {

    private String cookieId;

    private String sku;

    private long timeStamp;

    private long feedBack;

    public ZafulPcFeedBackCount(){}

    public ZafulPcFeedBackCount(String cookieId, String sku, long timeStamp, long feedBack) {
        this.cookieId = cookieId;
        this.sku = sku;
        this.timeStamp = timeStamp;
        this.feedBack = feedBack;
    }
}
