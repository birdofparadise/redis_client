package cn.mybop.redisclient;

import java.util.Properties;

import junit.framework.TestCase;

public class WriteAllReadAnyRedisClientTest extends TestCase {
	
	public IRedisClient getRedisClient() {
		Properties props = new Properties();
		props.put("pool.maxActive", "5");
		props.put("pool.maxIdle", "2");
		props.put("pool.maxWait", "2");
		props.put("pool.testOnBorrow", "false");
		props.put("pool.testOnReturn", "false");
		props.put("pool.testWhileIdle", "true");
		props.put("pool.timeBetweenEvictionRunsMillis", "60000");
		props.put("pool.minEvictableIdleTimeMillis", "-1");
		props.put("pool.softMinEvictableIdleTimeMillis", "1800000");
		props.put("pool.numTestsPerEvictionRun", "2");
		props.put("server.list", "192.168.137.100:11120,192.168.137.100:11121,192.168.137.100:11122");
		props.put("server.timeout", "2000");
		props.put("server.database", "0");
		props.put("serializable", "java");
		props.put("server.loadBalancer", "random");
		props.put("client.type", "writeallreadany");
		props.put("client.name", "writeallreadany");
		props.put("writeall.thread.pool", "3;5;2000;900");
		return RedisClientFactory.getClient(props);
	}
	
	public void testSetObject() {
		IRedisClient client = getRedisClient();
		client.setObject("writeallreadany", "writeallreadany");
		client.stop();
	}
	
	public void testGetObject() {
		IRedisClient client = getRedisClient();
		System.out.println(client.getObject("writeallreadany"));
		client.stop();
	}

}
