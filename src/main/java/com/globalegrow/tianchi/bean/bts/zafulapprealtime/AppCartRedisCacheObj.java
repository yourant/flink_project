package com.globalegrow.tianchi.bean.bts.zafulapprealtime;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@ToString
@Accessors(chain = true)
public class AppCartRedisCacheObj {

    private String key;

    private AppCartCacheValueInfo valueInfo;

}