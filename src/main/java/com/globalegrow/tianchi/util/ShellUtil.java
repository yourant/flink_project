package com.globalegrow.tianchi.util;

import com.globalegrow.tianchi.bean.ShellResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhougenggeng createTime  2019/9/29
 */
public class ShellUtil {
    private static final Logger logger = LoggerFactory.getLogger(ShellUtil.class);
    /**
     * 执行指定命令
     * @param command
     * @return
     */
    public static ShellResult execute(String command) {
        List<String> cmdComponents =new ArrayList<>() ;
        cmdComponents.add("sh");
        cmdComponents.add("-c");
        cmdComponents.add(command);

        /**进程句柄*/
        ProcessBuilder processBuilder = new ProcessBuilder(cmdComponents);
        Process process;
        try {
            process = processBuilder.start();
            process.waitFor();
        } catch (IOException ex) {
            String cmdStartExceptionMsg = String.format("启动命令失败, command:%s, 原因:%s", command, ex.toString());
            logger.error(cmdStartExceptionMsg, ex);

            return ShellResult.buildFailResult(cmdStartExceptionMsg);
        } catch (InterruptedException ex) {
            String cmdIntExceptionMsg = String.format("命令执行被中断, command:%s, 原因:%s", command, ex.toString());
            logger.error(cmdIntExceptionMsg, ex);

            return ShellResult.buildFailResult(cmdIntExceptionMsg);
        }

        ShellResult runResult = readResult(process);
        if (!runResult.isSuccess()) {
            String resultReadError = String.format("命令执行结果读取失败, command:%s, 原因:%s",
                command, runResult.getResultMessage());
            logger.error(resultReadError);
        }

        return runResult;
    }

    private static ShellResult readResult(Process process) {

        // 取得命令输出
        List<String> outStrList = readFromStream(process.getInputStream());
        if (outStrList == null) {
            return ShellResult.buildFailResult("读取命令标准输出发生异常");
        }

        // 取得命令错误
        List<String> errStrList = readFromStream(process.getErrorStream());
        if (errStrList == null) {
            return ShellResult.buildFailResult("读取命令标准错误发生异常");
        }

        return ShellResult.buildSuccessResult(process.exitValue(), outStrList, errStrList);
    }

    private static List<String> readFromStream(InputStream ins) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
        List<String> strList =new ArrayList<>() ;
        try {
            String line = reader.readLine();
            while (line != null) {
                strList.add(line);
                line = reader.readLine();
            }
        } catch (IOException ex) {
            String readError = "读取命令执行结果时出现读取IO异常";
            //logger.error(readError, ex);
            strList = null;
        } finally {
            try {
                reader.close();
            } catch (IOException ex) {
                String closeError = "读取命令执行结果时出现关闭IO异常";
                //logger.error(closeError, ex);
                strList = null;
            }
        }
        return strList;
    }
}
