package cn.mybop.redisclient.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

public class SentinelManager extends AbstractRedisManager {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SentinelManager.class);
	
	private String sentinels;
	
	private String masterName;
	
	private JedisSentinelPool jedisSentinelPool;
	
	public SentinelManager(String sentinels, String masterName, JedisPoolConfig poolConfig, int timeout, final String password, final int database) {
		super(poolConfig, timeout, password, database);
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
	public List<String> getAvailableServers() {
		throw new RedisException("不支持该操作");
	}
	
	@Override
	public boolean isAvailableServer(String server) {
		throw new RedisException("不支持该操作");
	}
	
	@Override
	public Jedis getJedis(String server) {
		throw new RedisException("不支持获取指定server的jedis连接");
	}

	@Override
	public Jedis getJedis() {
		return jedisSentinelPool.getResource();
	}
	
	@Override
	public Jedis getMasterJedis() {
		return jedisSentinelPool.getResource();
	}
	
	public boolean removeJedisPool(String server) {
		throw new RedisException("不支持该操作");
	}
	
	public void addJedisPool(String server) {
		throw new RedisException("不支持该操作");
	}

	@Override
	public String getServers() {
		return null;
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
