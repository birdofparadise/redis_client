package cn.mybop.redisclient.impl;

import java.util.Properties;

import cn.mybop.redisclient.RedisException;
import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;


public class SentinelClient extends AbstractRedisClient {
			
	public SentinelClient(Properties props) {
		super(props);
	}

	@Override
	public void removeUnavailableServer(Jedis jedis) {
		//not need do anything		
	}
	
	@Override
	protected void startInternal() {
		super.startInternal();}
	
	@Override
	public RedisManager initManager() {
		Properties props = getProps();
		String sentinels = props.getProperty(Constants.SENTINEL_LIST);
		String masterName = props.getProperty(Constants.SENTINEL_MASTER_NAME);
		if (Utils.isBlank(sentinels)) {
			throw new RedisException(Constants.SENTINEL_LIST + "参数为空");
		}
		if (Utils.isBlank(masterName)) {
			throw new RedisException(Constants.SENTINEL_MASTER_NAME + "参数为空");
		}
		JedisPoolConfig poolConfig = Utils.initPoolConfig(props);
		return new SentinelManager(sentinels, masterName, poolConfig, getTimeout(), getPassword(), getDatabase());
	}
	
	@Override
	protected void stopInternal() {
		super.stopInternal();
	}
	
}
