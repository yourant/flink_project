package com.globalegrow.tianchi.bean;

import lombok.Data;

import java.util.List;

/**
 * @author zhougenggeng createTime  2019/9/29
 */

@Data
public class ShellResult {
    private boolean success;
    private String resultMessage;
    private int exitValue;
    private List<String> stdOut;
    private List<String> stdErr;


    public ShellResult() {
    }

    public ShellResult(boolean success, String resultMessage, int exitValue, List<String> stdOut,
                       List<String> stdErr) {
        this.success = success;
        this.resultMessage = resultMessage;
        this.exitValue = exitValue;
        this.stdOut = stdOut;
        this.stdErr = stdErr;
    }

    public void setStdErr(List<String> stdErr) {
        this.stdErr = stdErr;
    }

    public static ShellResult buildSuccessResult(int exitValue,
                                                 List<String> stdOut,
                                                 List<String> stdErr) {
        ShellResult shellResult = new ShellResult();
        shellResult.setSuccess(true);
        shellResult.setResultMessage("");
        shellResult.setExitValue(exitValue);
        shellResult.setStdOut(stdOut);
        shellResult.setStdErr(stdErr);

        return shellResult;
    }

    public static ShellResult buildFailResult(String resultMessage) {
        ShellResult shellResult = new ShellResult();
        shellResult.setSuccess(false);
        shellResult.setResultMessage(resultMessage);

        return shellResult;
    }

}
