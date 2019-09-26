package com.globalegrow.tianchi.util;

import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author zhougenggeng createTime  2019/9/25
 */
public class HDFSUtils {

    /**
     * 获取ActiveNN节点
     *
     * @return
     */
    public static String getActiveNode() {
        Configuration hadoopConf = new Configuration();
        String activeNode = "";
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            activeNode = HAUtil.getAddressOfActive(fs).toString().split("/")[1];

        } catch (IOException e) {
            e.printStackTrace();
        }
        return activeNode;
    }

    /**
     * 删除本地Hadoop集群目录
     *
     * @param dataDir 要删除的路径
     */
    public static void deleteDataFiles(String dataDir) {
        Configuration hadoopConf = new Configuration();
        Path path = new Path(dataDir);
        try {
            FileSystem fs = path.getFileSystem(hadoopConf);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 初始化其它集群HDFS配置对象
     *
     * @param nameSpace 集群命名空间
     * @param nn1       nameNodeID1
     * @param nn2       nameNodeID2
     * @param nn1Addr   nn1对应 IP:port
     * @param nn2Addr   nn2对应 IP:port
     * @return
     */
    public static Configuration initHadoopConfig(String nameSpace, String nn1, String nn1Addr, String nn2,
                                                 String nn2Addr) {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://$nameSpace");
        hadoopConf.set("dfs.nameservices", nameSpace);
        hadoopConf.set("dfs.ha.namenodes.$nameSpace", "$nn1,$nn2");
        hadoopConf.set("dfs.namenode.rpc-address.$nameSpace.$nn1", nn1Addr);
        hadoopConf.set("dfs.namenode.rpc-address.$nameSpace.$nn2", nn2Addr);
        hadoopConf.set("dfs.client.failover.proxy.provider.$nameSpace",
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return hadoopConf;
    }

    /**
     * 检查doneFlag是否存在
     *
     * @param hadoopConf   初始化好的Hadoop对象
     * @param doneFlagPath doneFlag路径
     * @return
     */
    public static boolean checkDoneFlagExists(Configuration hadoopConf, String doneFlagPath) {
        Path path = new Path(doneFlagPath);
        FileSystem fs = null;
        boolean isExists = false;
        try {
            fs = path.getFileSystem(hadoopConf);
            isExists = fs.exists(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isExists;
    }

    /**
     * 检查doneFlag是否存在
     *
     * @param hadoopConf   初始化好的Hadoop对象
     * @param doneFlagPath doneFlag路径
     * @return
     */
    public static void deleteDoneFlag(Configuration hadoopConf, String doneFlagPath) {
        boolean isExist = checkDoneFlagExists(hadoopConf, doneFlagPath);
        Path path = new Path(doneFlagPath);
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(hadoopConf);
            if (isExist) {
                fs.delete(path, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*写入doneFlag
     * 如果存在则更新
     * @param hadoopConf   初始化好的Hadoop对象
     * @param doneFlagPath doneFlag路径
     */
    public static void writeDoneFlag(Configuration hadoopConf, String doneFlagPath) {
        boolean isExist = checkDoneFlagExists(hadoopConf, doneFlagPath);
        Path path = new Path(doneFlagPath);
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(hadoopConf);
            if (!isExist) {
                fs.mkdirs(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE));
            } else {
                deleteDoneFlag(hadoopConf, doneFlagPath);
                fs.mkdirs(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
