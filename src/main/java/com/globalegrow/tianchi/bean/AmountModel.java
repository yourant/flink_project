package com.globalegrow.tianchi.bean;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 18:50 2019/11/2
 * @Modified:
 */
public class AmountModel {

    private String sku;

    private double price;

    private int pam;

    @Override
    public String toString() {
        return "AmountModel{" +
                "sku='" + sku + '\'' +
                ", price=" + price +
                ", pam=" + pam +
                '}';
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getPam() {
        return pam;
    }

    public void setPam(int pam) {
        this.pam = pam;
    }
}
