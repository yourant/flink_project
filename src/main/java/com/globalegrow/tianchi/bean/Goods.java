package com.globalegrow.tianchi.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * @Description 商品明细维度实体类
 * @Author chongzi
 * @Date 2018/10/7 17:05
 **/
@Data
public class Goods implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 分区字段 ios / android
     */
    private String platform;

    /**
     * 国家
     */
    private String country;
    /**
     * 事件名称
     */
    private String eventName;
    /**
     * sku
     */
    private String sku;
    /**
     * 事件时间
     */
    private String eventTime;

    /**
     * 事件日期
     */
    private String eventDate;
    /**
     * 输入日期
     */
    private String intakeDate;
    /**
     * 创建订单价格
     */
    private Double createOrderPrice;
    /**
     * 创建商品销量
     */
    private Integer createOrderQuantity;

    /**
     * 创建订单总额
     */
    private Double gmv;
    /**
     * 支付订单价格
     */
    private Double purchaseOrderPrice;
    /**
     * 支付商品销量
     */
    private Integer purchaseOrderQuantity;

    /**
     * 支付订单总额
     */
    private Double purchaseOrderAmount;
    /**
     * 商品被曝光数量
     */
    private Integer skuExpCnt;
    /**
     * 商品被点击数量
     */
    private Integer skuHitCnt;
    /**
     * 商品被加购物车数量
     */
    private Integer skuCartCnt;
    /**
     * 商品被加收藏数量
     */
    private Integer skuMarkedCnt;
    /**
     * 创建订单总数
     */
    private Integer createOrderCnt;
    /**
     * 购买订单总数
     */
    private Integer purchaseOrderCnt;

    public Goods() {
    }

    public Goods(String platform, String country, String eventName, String sku, String eventTime, String eventDate, String intakeDate, Double createOrderPrice, Integer createOrderQuantity, Double gmv, Double purchaseOrderPrice, Integer purchaseOrderQuantity, Double purchaseOrderAmount, Integer skuExpCnt, Integer skuHitCnt, Integer skuCartCnt, Integer skuMarkedCnt, Integer createOrderCnt, Integer purchaseOrderCnt) {
        this.platform = platform;
        this.country = country;
        this.eventName = eventName;
        this.sku = sku;
        this.eventTime = eventTime;
        this.eventDate = eventDate;
        this.intakeDate = intakeDate;
        this.createOrderPrice = createOrderPrice;
        this.createOrderQuantity = createOrderQuantity;
        this.gmv = gmv;
        this.purchaseOrderPrice = purchaseOrderPrice;
        this.purchaseOrderQuantity = purchaseOrderQuantity;
        this.purchaseOrderAmount = purchaseOrderAmount;
        this.skuExpCnt = skuExpCnt;
        this.skuHitCnt = skuHitCnt;
        this.skuCartCnt = skuCartCnt;
        this.skuMarkedCnt = skuMarkedCnt;
        this.createOrderCnt = createOrderCnt;
        this.purchaseOrderCnt = purchaseOrderCnt;
    }

    @Override
    public String toString() {
        return  platform +
                "," + country +
                "," + eventName  +
                "," + sku +
                "," + eventTime +
                "," + eventDate +
                "," + intakeDate +
                "," + createOrderPrice +
                "," + createOrderQuantity +
                "," + gmv +
                "," + purchaseOrderPrice +
                "," + purchaseOrderQuantity +
                "," + purchaseOrderAmount +
                "," + skuExpCnt +
                "," + skuHitCnt +
                "," + skuCartCnt +
                "," + skuMarkedCnt +
                "," + createOrderCnt +
                "," + purchaseOrderCnt ;
    }
}
