package com.globalegrow.tianchi.bean;

import lombok.Data;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 21:41 2019/11/10
 * @Modified:
 */
@Data
public class PhpOrderSum {

    private String eventType;
    private String sku;
    private double amount;
    private String timeStamp;
    private String platform;
//    private String country_number;

    public PhpOrderSum(){}

    public PhpOrderSum(String eventType, String sku, double amount, String timeStamp, String platform) {
        this.eventType = eventType;
        this.sku = sku;
        this.amount = amount;
        this.timeStamp = timeStamp;
        this.platform = platform;
    }
}
