package com.globalegrow.tianchi.event;

/**
 * @Description 发布帖子（普通帖子或穿搭）
 * @Author chongzi
 * @Date 2018/10/7 17:31
 **/
public class AfPost {
    /**
     * 发布帖子的帖子id
     */
    private String af_content_id;

    public String getAf_content_id() {
        return af_content_id;
    }

    public void setAf_content_id(String af_content_id) {
        this.af_content_id = af_content_id;
    }
}
