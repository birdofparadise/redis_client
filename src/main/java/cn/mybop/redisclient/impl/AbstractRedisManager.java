package cn.mybop.redisclient.impl;

import java.sql.Timestamp;

import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.lifecycle.LifecycleBase;
import redis.clients.jedis.JedisPoolConfig;

public abstract class AbstractRedisManager extends LifecycleBase implements RedisManager {
	
	private JedisPoolConfig poolConfig;

	private int timeout;
	
	private String password;
	
	private int database;
	
	//启动时间
	private Timestamp startTime;
	
	public AbstractRedisManager() {
	}

	public AbstractRedisManager(JedisPoolConfig poolConfig, int timeout, String password, int database) {
		this.poolConfig = poolConfig;
		this.timeout = timeout;
		this.password = password;
		this.database = database;
	}
	
	public JedisPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(JedisPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	@Override
	public Timestamp getStartTime() {
		return startTime;
	}

	@Override
	protected void startInternal() {
		startTime = new Timestamp(System.currentTimeMillis());
	}

	@Override
	protected void stopInternal() {
		
	}

}
