package com.globalegrow.tianchi.transformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.EmpMQInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * <p>EmpMQFlatMap 类</p>
 * Author: Ding Jian
 * Date: 2019-11-14 16:59:59
 */
public class EmpMQFlatMap implements FlatMapFunction<String, EmpMQInfo> {


    @Override
    public void flatMap(String in, Collector<EmpMQInfo> out) {

        //解析JSON
        HashMap<String, Object> map = JSON.parseObject(in, new TypeReference<HashMap<String, Object>>() {
        });

        String site = (String) map.get("site");
        String batchNo = (String) map.get("batchNo");
        List<String> userIds = (List<String>) map.get("user_ids");
        Integer id = (Integer) map.get("id");
        Integer pushTime = (Integer) map.get("push_time");

        EmpMQInfo empMQInfo = new EmpMQInfo();
        empMQInfo.setSite(site);
        empMQInfo.setBatchNo(batchNo);
        empMQInfo.setPushTime(pushTime);
        empMQInfo.setId(id);

        List<String> list = new ArrayList<>();
        for (String s : userIds) {
            String userId = (s.replace("|", ""));
            list.add(userId);
        }
        empMQInfo.setUserIdList(list);

        out.collect(empMQInfo);

    }
}
