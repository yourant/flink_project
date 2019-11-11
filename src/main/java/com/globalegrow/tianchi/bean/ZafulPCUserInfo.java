package com.globalegrow.tianchi.bean;

import java.math.BigInteger;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 10:42 2019/10/31
 * @Modified:
 */
public class ZafulPCUserInfo {

    private String cookieId;
    private String eventType;
    private String eventValue;
    private String timeStamp;

    public ZafulPCUserInfo(){}

    @Override
    public String toString() {
        return "ZafulPCUserInfo{" +
                "cookieId='" + cookieId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventValue='" + eventValue + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }

    public String getCookieId() {
        return cookieId;
    }

    public void setCookieId(String cookieId) {
        this.cookieId = cookieId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventValue() {
        return eventValue;
    }

    public void setEventValue(String eventValue) {
        this.eventValue = eventValue;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }
}
