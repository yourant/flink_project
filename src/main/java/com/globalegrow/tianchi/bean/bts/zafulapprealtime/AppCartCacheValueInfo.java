package com.globalegrow.tianchi.bean.bts.zafulapprealtime;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@ToString
@Accessors(chain = true)
public class AppCartCacheValueInfo {


    /**
     *平台
     */
    private String btsPlatform;

    /**
     *语言
     */
    private String btsAfLang;

    /**
     *国家站
     */
    private String btsAfNationalCode;

    /**
     *国家
     */
    private String btsAfCountryCode;


    /**
     *排序
     */
    private String btsAfSort;


    /**
     *实验id
     */
    private String btsPlancode;

    /**
     *实验id
     */
    private String btsPlanid;


    /**
     *实验版本id
     */
    private String btsVersionid;

    /**
     *分桶id
     */
    private String btsBucketid;
    /**
     * 时间戳
     */
    private long timestamp;

    /**
     * 站点
     */
    private String site;

    /**
     * 设备id
     */
    private String appsflyerDeviceId;

    /**
     * 场景筛选
     */
    private String afInnerMediasource;

    /**
     * 场景筛选缩写
     */
    private String afInnerMediasourceAbridge;
    /**
     * 事件sku
     */
    private String afContentId;

}
