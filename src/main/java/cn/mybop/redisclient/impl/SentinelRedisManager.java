package cn.mybop.redisclient.impl;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

public class SentinelRedisManager extends RedisManagerImpl {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SentinelRedisManager.class);
	
	private String sentinels;
	
	private String masterName;
	
	private JedisSentinelPool jedisSentinelPool;
	
	public SentinelRedisManager(String servers, String sentinels, String masterName, JedisPoolConfig poolConfig, int timeout, final String password, final int database, String loadBalancer) {
		super(servers, poolConfig, timeout, password, database, loadBalancer);
		this.sentinels = sentinels;
		this.masterName = masterName;
	}
	
	@Override
	public String getSentinels() {
		return sentinels;
	}
	
	@Override
	public String getMasterName() {
		return masterName;
	}
	
	@Override
	public Jedis getMasterJedis() {
		return jedisSentinelPool.getResource();
	}
	
	@Override
	protected void startInternal() {
		super.startInternal();		
		String[] tmpSentinels = sentinels.split(",");
		if (tmpSentinels == null || tmpSentinels.length < 1) {
			throw new RedisException("sentinel服务器列表为空");
		}
		Set<String> sentinelSet = new HashSet<String>(tmpSentinels.length);
		for (int i = 0; i < tmpSentinels.length; i++) {
			sentinelSet.add(tmpSentinels[i]);
		}
		jedisSentinelPool = new JedisSentinelPool(masterName, sentinelSet, getPoolConfig(), getTimeout(), getPassword(), getDatabase());

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("sentinel服务器[" + sentinels + "-" + masterName + "]启动完成");
		}
	}

	@Override
	protected void stopInternal() {	
		if (jedisSentinelPool != null) {
			try {
				jedisSentinelPool.destroy();
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("sentinel[" + sentinels + "-" + masterName + "]关闭出现异常", e);
				}
			} finally {
				jedisSentinelPool = null;
			}
		}		
		super.stopInternal();
	}
	
}
