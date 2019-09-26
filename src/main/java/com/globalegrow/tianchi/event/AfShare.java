package com.globalegrow.tianchi.event;

/**
 * @Description 用户通过 facebook或messger进行分享
 * @Author chongzi
 * @Date 2018/10/7 17:31
 **/
public class AfShare {
    /**
     * 分享至哪里，枚举值有:Shared on facebook、Shared on messager
     */
    private String af_content_type;
    /**
     * 商品sku(切勿使用goods_id)
     */
    private String af_content_id;

    public String getAf_content_id() {
        return af_content_id;
    }

    public void setAf_content_id(String af_content_id) {
        this.af_content_id = af_content_id;
    }
}
