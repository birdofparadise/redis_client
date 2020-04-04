package cn.mybop.redisclient.impl;

import java.util.Properties;

import cn.mybop.redisclient.RedisException;
import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import redis.clients.jedis.JedisPoolConfig;

public class SentinelRedisClient extends AdvancedRedisClient {
	
	public SentinelRedisClient(Properties props) {
		super(props);
	}

	@Override
	protected void startInternal() {
		super.startInternal();
	}
	
	public RedisManager initManager() {
		Properties props = getProps();
		String servers = props.getProperty(Constants.SERVER_LIST);
		String sentinels = props.getProperty(Constants.SENTINEL_LIST);
		String masterName = props.getProperty(Constants.SENTINEL_MASTER_NAME);
		if (Utils.isBlank(servers)) {
			throw new RedisException(Constants.SERVER_LIST + "参数为空");
		}
		if (Utils.isBlank(sentinels)) {
			throw new RedisException(Constants.SENTINEL_LIST + "参数为空");
		}
		if (Utils.isBlank(masterName)) {
			throw new RedisException(Constants.SENTINEL_MASTER_NAME + "参数为空");
		}
		String loadBalancer = Constants.DEFAULT_LOADBALANCER;
		if (Utils.isNotBlank(props.getProperty(Constants.SERVER_LOADBALANCER))) {
			loadBalancer = props.getProperty(Constants.SERVER_LOADBALANCER);
		}
		JedisPoolConfig poolConfig = Utils.initPoolConfig(props);
		return new SentinelRedisManager(servers, sentinels, masterName, poolConfig, getTimeout(), getPassword(), getDatabase(), loadBalancer);
	}

	@Override
	protected void stopInternal() {
		super.stopInternal();
	}

}
