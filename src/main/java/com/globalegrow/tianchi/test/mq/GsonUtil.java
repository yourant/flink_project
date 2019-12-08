package com.globalegrow.tianchi.test.mq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtil {

	public static Gson getGson() {
		GsonBuilder builder = new GsonBuilder();
		builder.setDateFormat("yyyy-MM-dd HH:mm:ss");
		return builder.disableHtmlEscaping().create();
	}
}

