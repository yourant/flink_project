package com.globalegrow.tianchi.bean.bts.zafulapprealtime;

import lombok.Data;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 18:30 2019/11/10
 * @Modified:
 */
@Data
public class PcEventBehahvior {

    private String eventType;

    private String sku;

    private long timeStamp;

    private String platform;

    private String countryCode;

    public PcEventBehahvior(){}

    public PcEventBehahvior(String eventType, String sku, long timeStamp, String platform, String countryCode){
        this.eventType = eventType;
        this.sku = sku;
        this.timeStamp = timeStamp;
        this.platform = platform;
        this.countryCode = countryCode;
    }

}
