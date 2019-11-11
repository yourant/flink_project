package com.globalegrow.tianchi.bean;

import lombok.Data;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 0:09 2019/11/11
 * @Modified:
 */
@Data
public class ZafulPcSkuBehavior {

    private String cookieId;
    private String userId;
    private String eventType;
    private String sku;
    private long timeStamp;

    public ZafulPcSkuBehavior(){}

    public ZafulPcSkuBehavior(String cookieId, String userId, String eventType, String sku, long timeStamp) {
        this.cookieId = cookieId;
        this.userId = userId;
        this.eventType = eventType;
        this.sku = sku;
        this.timeStamp = timeStamp;
    }
}
