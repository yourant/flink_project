package com.globalegrow.tianchi.bean.bts.zafulapprealtime;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * @author zhougenggeng createTime  2019/10/22
 */
@Data
@ToString
@Accessors(chain = true)
public class AppOrderModel {
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

    /**
     * 事件名称
     */
    private String eventName;

    /**
     * 修改颜色和尺寸
     */
    private String afChangedSizeOrColor;

    /**
     * 商品数量
     */
    private String afQuantity;

    /**
     * 商品数量
     */
    private String afPrice;
}
