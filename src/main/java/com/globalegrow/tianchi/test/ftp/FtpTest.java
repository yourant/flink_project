package com.globalegrow.tianchi.test.ftp;

import com.globalegrow.tianchi.util.FTPUtil;

/**
 * @author zhougenggeng createTime  2019/9/29
 */
public class FtpTest {
    public static void main(String[] args) {

        boolean file = FTPUtil.uploadLocalFile("zhougenggeng", "C:/Users/Administrator/Desktop", "新员工入职指引(1).mht");
    }
}
