package com.globalegrow.tianchi.bean;

import lombok.Data;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 20:29 2019/11/10
 * @Modified:
 */
@Data
public class PhpOrderBehavior {

    private String cookieId;
    private String userId;
    private String eventType;
    private String sku;
    private double price;
    private int pam;
    private long timeStamp;
    private String platform;
    private String country_number;

    public PhpOrderBehavior(){}

    public PhpOrderBehavior(String cookieId, String userId, String eventType, String sku, double price, int pam, long timeStamp, String platform, String country_number) {
        this.cookieId = cookieId;
        this.userId = userId;
        this.eventType = eventType;
        this.sku = sku;
        this.price = price;
        this.pam = pam;
        this.timeStamp = timeStamp;
        this.platform = platform;
        this.country_number = country_number;
    }
}
