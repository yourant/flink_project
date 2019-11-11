package com.globalegrow.tianchi.bean;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 17:48 2019/10/31
 * @Modified:
 */
public class SubEventField {

    private String sku;

    private String rank;

    private String price;

    private String discount_mark;

    private String marketType;

    private String fmd;

    private String sort;

    private String p;

    private String skty;

    @Override
    public String toString() {
        return "SubEventField{" +
                "sku='" + sku + '\'' +
                ", rank='" + rank + '\'' +
                ", price='" + price + '\'' +
                ", discount_mark='" + discount_mark + '\'' +
                ", marketType='" + marketType + '\'' +
                ", fmd='" + fmd + '\'' +
                ", sort='" + sort + '\'' +
                ", p='" + p + '\'' +
                ", skty='" + skty + '\'' +
                '}';
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getDiscount_mark() {
        return discount_mark;
    }

    public void setDiscount_mark(String discount_mark) {
        this.discount_mark = discount_mark;
    }

    public String getMarketType() {
        return marketType;
    }

    public void setMarketType(String marketType) {
        this.marketType = marketType;
    }

    public String getFmd() {
        return fmd;
    }

    public void setFmd(String fmd) {
        this.fmd = fmd;
    }

    public String getSort() {
        return sort;
    }

    public void setSort(String sort) {
        this.sort = sort;
    }

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public String getSkty() {
        return skty;
    }

    public void setSkty(String skty) {
        this.skty = skty;
    }
}
