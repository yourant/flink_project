package com.globalegrow.tianchi.event;

/**
 * @Description 商品曝光
 * @Author chongzi
 * @Date 2018/10/7 17:31
 **/
public class AfSkuView {

    /**
     * "a"对应页面附录的编号
     */
    private String af_page;
    /**
     * "a01"对应页面附录的编号
     */
    private String af_page_code;
    /**
     * 商品具体的页面位置
     */
    private String af_page_position;
    /**
     * 商品sku，多个用,分隔
     */
    private String af_content_ids;

    public String getAf_page() {
        return af_page;
    }

    public void setAf_page(String af_page) {
        this.af_page = af_page;
    }

    public String getAf_page_code() {
        return af_page_code;
    }

    public void setAf_page_code(String af_page_code) {
        this.af_page_code = af_page_code;
    }

    public String getAf_page_position() {
        return af_page_position;
    }

    public void setAf_page_position(String af_page_position) {
        this.af_page_position = af_page_position;
    }

    public String getAf_content_ids() {
        return af_content_ids;
    }

    public void setAf_content_ids(String af_content_ids) {
        this.af_content_ids = af_content_ids;
    }
}
