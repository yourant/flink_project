package com.globalegrow.tianchi.test.hive;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpUtil;
import com.globalegrow.tianchi.bean.AppDataModel;
import com.globalegrow.tianchi.bean.Tokenizer;
import com.globalegrow.tianchi.util.HDFSUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.orc.OrcTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.hadoop.conf.Configuration;
import cn.hutool.core.thread.ThreadUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author zhougenggeng createTime  2019/9/26
 */
public class Hive2hive {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //see.setParallelism(parameterTool.getInt("job.parallelism", 1));
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parameterTool.getInt("job.parallelism", 1));
        env.getConfig().setGlobalJobParameters(parameterTool);
        ////获取时间
        //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        //Calendar c = Calendar.getInstance();
        //c.setTime(new Date());
        //c.add(Calendar.HOUR, +8);
        //c.add(Calendar.DATE, -1);
        //String yesterday = df.format(c.getTime());
        // 获取昨天的年
        String yesterdayYear = DateUtil.format(DateUtil.yesterday(), "yyyy");
        // 获取昨天的月
        String yesterdayMonth = DateUtil.format(DateUtil.yesterday(), "MM");
        // 获取昨天的日
        String yesterday = DateUtil.format(DateUtil.yesterday(), "dd");

        String today = DateUtil.today();


        // 外网
        //String kylinUrl = "http://35.153.241.61:8095/cube-build/manual";
        String kylinUrl = "http://172.31.33.169:8095/cube-build/manual";
        String kylinTaskName = "dy_gb_goods_cube";
        String kylinType = "2";
        String kylinStartTime = today + " 00:00:00";
        String kylinEndTime = DateUtil.format(DateUtil.tomorrow(),"yyyy-MM-dd") + " 00:00:00";


        if (parameterTool.has("yesterdayYear")) {
            yesterdayYear = parameterTool.get("yesterdayYear");
        }
        if (parameterTool.has("yesterdayMonth")) {
            yesterdayMonth = parameterTool.get("yesterdayMonth");
        }
        if (parameterTool.has("yesterday")) {
            yesterday = parameterTool.get("yesterday");
        }

        if (parameterTool.has("kylinUrl")) {
            kylinUrl = parameterTool.get("kylinUrl");
        }

        if (parameterTool.has("kylinTaskName")) {
            kylinTaskName = parameterTool.get("kylinTaskName");
        }

        if (parameterTool.has("kylinType")) {
            kylinType = parameterTool.get("kylinType");
        }

        if (parameterTool.has("kylinStartTime")) {
            kylinStartTime = parameterTool.get("kylinStartTime");
        }

        if (parameterTool.has("kylinEndTime")) {
            kylinEndTime = parameterTool.get("kylinEndTime");
        }

        if (parameterTool.has("today")) {
            today = parameterTool.get("today");
        }
        //获取大数据的hadoopConf
        Configuration hadoopConf = HDFSUtils.initHadoopConfig("glbgnameservice", "namenode113", "172.31.57.86:8020",
            "namenode203", "172.31.20.96:8020");
        // 获取Active的NameNode的IP:port
        String hdfsActive = HDFSUtils.getActiveNode(hadoopConf);
        System.out.println("hdfsActive:" + hdfsActive);
        String defaultFS = "hdfs://" + hdfsActive + "/";
        String inputFliePath = "hdfs://" + hdfsActive + "/bigdata/ods/log_clean/ods_app_burial_log/" + yesterdayYear
            + "/" + yesterdayMonth + "/" + yesterday + "/";
        String checkUrl = "/bigdata/log_clean/job/done_flag/" + today;
        if (parameterTool.has("checkUrl")) {
            checkUrl = parameterTool.get("checkUrl");
        }
        boolean isExit ;
        while (true) {
            isExit = HDFSUtils.checkDoneFlagExists(hadoopConf, checkUrl);
            if (isExit) {
                break;
            }
            ThreadUtil.safeSleep(60000);
        }
        if (isExit) {
            System.out.println(today + "有推数");
        } else {
            System.out.println(today + "没有推数");
        }
        String querySql = "select platform,country_code,event_name,event_value,event_time from goods_event " +
            "where event_name in('af_sku_view','af_view_product','af_add_to_bag','af_add_to_wishlist','af_create_order_success','af_search','af_purchase') " +
            "and lower(app_name) like '%gearbest%' ";
        String outputtFliePath = "hdfs:///user/hadoop/flink/gb/goods/wash/"+yesterdayYear+"/"+yesterdayMonth+"/"+yesterday+"/detail.csv";

        if (parameterTool.has("defaultFS")) {
            defaultFS = parameterTool.get("defaultFS");
        }
        if (parameterTool.has("inputFliePath")) {
            inputFliePath = parameterTool.get("inputFliePath");
        }
        if (parameterTool.has("querySql")) {
            querySql = parameterTool.get("querySql");
        }
        if (parameterTool.has("outputtFliePath")) {
            outputtFliePath = parameterTool.get("outputtFliePath");
        }


        //EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(see, bsSettings);
        //EnvironmentSettings btSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        Configuration config = new Configuration();

        config.set("fs.defaultFS", defaultFS);
        OrcTableSource orcTableSource = OrcTableSource.builder()
            .path(inputFliePath)
            .forOrcSchema("struct<app_name:string,platform:string,country_code:string,event_name:string,event_value:string,event_time:string>")
            .withConfiguration(config)
            .build();
        tableEnv.registerTableSource("goods_event", orcTableSource);
        Table table = tableEnv.sqlQuery(querySql);
        DataSet<AppDataModel> dataStream = tableEnv.toDataSet(table, AppDataModel.class);
        DataSet<String> stringDataStream= dataStream.flatMap(new Tokenizer());

        Table table1 = tableEnv.fromDataSet(stringDataStream);
        TableSink csvSink = new CsvTableSink(outputtFliePath,",",1, FileSystem.WriteMode.OVERWRITE);
        table1.insertInto(outputtFliePath);
        env.execute(parameterTool.get("job-name","dy-gb-goods-wash-"+yesterdayYear+"-"+yesterdayMonth+"-"+yesterday));

        String runningShellFile="dy-gb-goods-wash.sh";
        String shellFileDir="/usr/local/services/hive";
        String shellParam1 = "alter table dy_gd.gb_goods_wash add PARTITION(year='"+yesterdayYear+"',month='"+yesterdayMonth+"',day='"+yesterday+"') location 'hdfs:///user/hadoop/flink/gb/goods/wash/"+yesterdayYear+"/"+yesterdayMonth+"/"+yesterday+"';";
        String shellParam2 = "shell2";
        String shellParam3 = "shell3";
        if (parameterTool.has("runningShellFile")) {
            runningShellFile = parameterTool.get("runningShellFile");
        }
        if (parameterTool.has("shellFileDir")) {
            shellFileDir = parameterTool.get("shellFileDir");
        }
        if (parameterTool.has("shellParam1")) {
            shellParam1 = parameterTool.get("shellParam1");
        }
        if (parameterTool.has("shellParam2")) {
            shellParam2 = parameterTool.get("shellParam2");
        }
        if (parameterTool.has("shellParam3")) {
            shellParam3 = parameterTool.get("shellParam3");
        }
        ProcessBuilder pb = new ProcessBuilder("./" + runningShellFile, shellParam1,
            shellParam2, shellParam3);//可以在RUNNING_SHELL_FILE脚本中直接通过$1,$2,$3分别拿到的参数。
        pb.directory(new File(shellFileDir));
        int runningStatus = 0;//运行状态，0标识正常
        String s = null;
        try {
            Process p = pb.start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while ((s = stdInput.readLine()) != null) {
                System.out.println("stdInput.readLine()="+s);
            }
            while ((s = stdError.readLine()) != null) {
                System.out.println("stdError.readLine()="+s);
            }
            try {
                runningStatus = p.waitFor();
            } catch (InterruptedException e) {
            }

        } catch (IOException e) {
        }
        if (runningStatus != 0) {
        }
        Map<String, Object> paramMap = new LinkedHashMap<>();
        paramMap.put("taskName",kylinTaskName);
        paramMap.put("type",kylinType);
        paramMap.put("startTime",kylinStartTime);
        paramMap.put("endTime",kylinEndTime);
        HttpUtil.post(kylinUrl,paramMap);


    }
}
