package com.globalegrow.tianchi.event;

/**
 * @Description 用户执行搜索操作（包含有普通搜索或图片搜索）
 * @Author chongzi
 * @Date 2018/10/7 17:29
 **/
public class AfSearch {

    /**
     * 枚举值有：normal_search（普通搜索）、pic_search（图片搜索）
     */
    private String af_search_page;
    /**
     * 如af_search_page为pic_search,则为no keyword，如如af_search_page为pic_search为normal_search，则为对应的搜索关键字
     */
    private String af_content_type;

    public String getAf_search_page() {
        return af_search_page;
    }

    public void setAf_search_page(String af_search_page) {
        this.af_search_page = af_search_page;
    }

    public String getAf_content_type() {
        return af_content_type;
    }

    public void setAf_content_type(String af_content_type) {
        this.af_content_type = af_content_type;
    }
}
