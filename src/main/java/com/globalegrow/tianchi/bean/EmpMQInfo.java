package com.globalegrow.tianchi.bean;

import lombok.Data;

import java.util.List;

/**
 * <p>MQ信息 实体类</p>
 * Author: Ding Jian
 * Date: 2019-11-14 17:02:11
 */
@Data
public class EmpMQInfo {

    //站点名称
    private String site;

    //批次号
    private String batchNo;

    //人群ID
    private Integer id;

    //推送时间
    private Integer pushTime;

    //用户id集合
    private List<String> userIdList;

}
