package com.globalegrow.tianchi.event;

/**
 * @Description 用户注册或登录成功后调用（包含FACEBOOK,GOOGLE登录）
 * @Author chongzi
 * @Date 2018/10/7 17:27
 **/
public class AfSignUp {

    /**
     * 枚举值有Email,Google,Facebook
     */
    private String af_content_type;

    public String getAf_content_type() {
        return af_content_type;
    }

    public void setAf_content_type(String af_content_type) {
        this.af_content_type = af_content_type;
    }
}
