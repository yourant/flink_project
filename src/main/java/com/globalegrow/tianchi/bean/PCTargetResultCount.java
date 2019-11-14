package com.globalegrow.tianchi.bean;

import lombok.Data;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 14:17 2019/11/1
 * @Modified:
 */
@Data
public class PCTargetResultCount {

    private String eventType;

    private String sku;

    private String timeStamp;

    private String platform;

    private String countryCode;

    private long viewCount;

    public PCTargetResultCount(){}

    public PCTargetResultCount(String eventType, String sku, String timeStamp, String platform, String countryCode,long viewCount){
        this.eventType = eventType;
        this.sku = sku;
        this.timeStamp = timeStamp;
        this.platform = platform;
        this.countryCode = countryCode;

        this.viewCount = viewCount;
    }


}
