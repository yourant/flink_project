package com.globalegrow.tianchi.event;

/**
 * @Description 用户从购物车进入checkout页，checkout页需要获取接口数据成功后触发 此事件（修改地址、使用优惠券也会调用）
 * @Author chongzi
 * @Date 2018/10/7 17:28
 **/
public class AfProcessToPay {

    /**
     * 固定值，必须要为USD
     */
    private String af_currency;
    /**
     * 此订单（其实未真正生成订单）中的商品的价格，多个用,分隔
     */
    private String af_price;
    /**
     * 此订单（其实未真正生成订单）中的商品的sku(切勿使用goods_id)，多个用,分隔
     */
    private String af_content_id;
    /**
     * 此订单（其实未真正生成订单）中的商品的购买数量，多个用,分隔
     */
    private String af_quantity;
    /**
     * 固定值，必须为prodcut
     */
    private String af_content_type;

    public String getAf_currency() {
        return af_currency;
    }

    public void setAf_currency(String af_currency) {
        this.af_currency = af_currency;
    }

    public String getAf_price() {
        return af_price;
    }

    public void setAf_price(String af_price) {
        this.af_price = af_price;
    }

    public String getAf_content_id() {
        return af_content_id;
    }

    public void setAf_content_id(String af_content_id) {
        this.af_content_id = af_content_id;
    }

    public String getAf_quantity() {
        return af_quantity;
    }

    public void setAf_quantity(String af_quantity) {
        this.af_quantity = af_quantity;
    }

    public String getAf_content_type() {
        return af_content_type;
    }

    public void setAf_content_type(String af_content_type) {
        this.af_content_type = af_content_type;
    }
}
