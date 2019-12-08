package com.globalegrow.tianchi.transformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.HashMap;
import java.util.List;

/**
 * <p>过滤操作</p>
 * Author: Ding Jian
 * Date: 2019-11-20 22:02:53
 */
public class EmpMQFilter implements FilterFunction<String> {
    @Override
    public boolean filter(String in) throws Exception {
        boolean flag = true;

        if (StringUtils.isBlank(in)) {
            return false;
        }

        //解析JSON
        HashMap<String, Object> map = JSON.parseObject(in, new TypeReference<HashMap<String, Object>>() {
        });

        String batchNo = (String) map.get("batchNo");
        String site = (String) map.get("site");
        List<String> userIds = (List<String>) map.get("user_ids");
        Integer id = (Integer) map.get("id");


        if (site == null || batchNo == null || userIds.size() == 0 || id == null) {
            flag = false;
        }

        return flag;
    }
}
