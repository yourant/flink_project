package com.globalegrow.tianchi.test.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeoutException;

public class MQConn implements Serializable{
	public Channel getChan() throws IOException, TimeoutException{
		ConnectionFactory factory = new ConnectionFactory();
 		factory.setHost("kx.rabbitmq.hqygou.com");
		factory.setPort(5672);
		factory.setUsername("ips_product");
		factory.setPassword("TubATzGxu");
		factory.setVirtualHost("IPS");
		Connection connection = factory.newConnection();
		return connection.createChannel();
	}

}
