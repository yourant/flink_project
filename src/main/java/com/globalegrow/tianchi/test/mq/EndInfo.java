package com.globalegrow.tianchi.test.mq;

import java.io.Serializable;

public class EndInfo implements Serializable{
	
	/**
	 * 服装站和电子站 FZ GB
	 */
	private String site;
	
	/**
	 * 0 check决策系统数据是否到位
     * 1 spark计算开始
     * 2 spark计算结束
     * 3 写入es开始
     * 4 写入es结束
     * 5 抽数开始
     * 6 抽数结束
	 */
	private int status;
	
    private long time;
	
	private String date;

	public String getSite() {
		return site;
	}

	public void setSite(String site) {
		this.site = site;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return "EndInfo [site=" + site + ", status=" + status + ", time=" + time + ", date=" + date + "]";
	}

}
