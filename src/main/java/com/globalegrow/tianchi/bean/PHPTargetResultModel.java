package com.globalegrow.tianchi.bean;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 9:49 2019/11/3
 * @Modified:
 */
public class PHPTargetResultModel{
    private String time_stamp;
    private String platform;
    private String countryCode;
    private String event_value;
    private Long order_nums;
    private Long purchase_nums;
    private double order_gmv;
    private double purchase_gmv;

    @Override
    public String toString() {
        return "PHPTargetResultModel{" +
                "time_stamp='" + time_stamp + '\'' +
                ", platform='" + platform + '\'' +
                ", countryCode='" + countryCode + '\'' +
                ", event_value='" + event_value + '\'' +
                ", order_nums=" + order_nums +
                ", purchase_nums=" + purchase_nums +
                ", order_gmv=" + order_gmv +
                ", purchase_gmv=" + purchase_gmv +
                '}';
    }

    public String getTime_stamp() {
        return time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = time_stamp;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getEvent_value() {
        return event_value;
    }

    public void setEvent_value(String event_value) {
        this.event_value = event_value;
    }

    public Long getOrder_nums() {
        return order_nums;
    }

    public void setOrder_nums(Long order_nums) {
        this.order_nums = order_nums;
    }

    public Long getPurchase_nums() {
        return purchase_nums;
    }

    public void setPurchase_nums(Long purchase_nums) {
        this.purchase_nums = purchase_nums;
    }

    public double getOrder_gmv() {
        return order_gmv;
    }

    public void setOrder_gmv(double order_gmv) {
        this.order_gmv = order_gmv;
    }

    public double getPurchase_gmv() {
        return purchase_gmv;
    }

    public void setPurchase_gmv(double purchase_gmv) {
        this.purchase_gmv = purchase_gmv;
    }
}
