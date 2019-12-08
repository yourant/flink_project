package com.globalegrow.tianchi.test.mq;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;

public class FZEndinfoRunner {

	public static void main(String[] args) {
		EndInfo info = new EndInfo();
		info.setSite("FZ");
		info.setStatus(6);
		info.setTime(System.currentTimeMillis());
		info.setDate(new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss").format(new Date()));
		
		Gson gson = GsonUtil.getGson();
		
		MQConn conn = new MQConn();
		
			Channel chan = null;
			try {
				chan = conn.getChan();
				chan.basicPublish("", "esEndInfo_IPS", null, gson.toJson(info).getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}finally{
				try {
					chan.getConnection().close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		
	}

}
