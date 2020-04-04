package cn.mybop.redisclient.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisException;
import cn.mybop.redisclient.balance.LoadBalancer;
import cn.mybop.redisclient.balance.RandomLoadBalancer;
import cn.mybop.redisclient.balance.RoundRobinLoadBalancer;
import cn.mybop.redisclient.common.Constants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class AdvancedRedisManager extends AbstractRedisManager {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedRedisManager.class);
	//全量服务列表
	private String servers;
	
	//负载均衡策略
	private LoadBalancer loadBalancer;
	
	private Map<String, JedisPool> jedisPools;
	
	//可用服务列表
	private List<String> availableServers;
	
	private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	
	public AdvancedRedisManager(String servers, JedisPoolConfig poolConfig, int timeout, final String password, final int database, String loadBalancer) {
		super(poolConfig, timeout, password, database);
		this.servers = servers;
		if (Constants.Loadbalancer.ROUNDROBIN.equalsIgnoreCase(loadBalancer)) {
			this.loadBalancer = new RoundRobinLoadBalancer();
		} else {
			this.loadBalancer = new RandomLoadBalancer();
		}
	}

	@Override
	public String getServers() {
		return servers;
	}
		
	@Override
	public List<String> getAvailableServers() {
		rwl.readLock().lock();
		try {
			List<String> tmpServers = new ArrayList<String>(availableServers.size());
			for (int i = 0; i < availableServers.size(); i++) {
				tmpServers.add(availableServers.get(i));
			}
			return tmpServers;
		} finally {
			rwl.readLock().unlock();
		}
	}
	
	@Override
	public boolean isAvailableServer(String server) {
		rwl.readLock().lock();
		try {
			return availableServers.contains(server);
		} finally {
			rwl.readLock().unlock();
		}
	}
	
	@Override
	public Jedis getJedis(String server) {
		return jedisPools.get(server).getResource();
	}

	@Override
	public Jedis getJedis() {
		String server = null;
		rwl.readLock().lock();
		try {
			if (availableServers.size() == 0) {
				throw new JedisConnectionException("无可用的redis服务器");
			}
			server = availableServers.get(loadBalancer.selectServer(availableServers));
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("共发现" + availableServers.size() + "个可用redis服务器," + "本次使用redis服务器为" + server);
			}
		} finally {
			rwl.readLock().unlock();
		}
		return jedisPools.get(server).getResource();
	}
	
	@Override
	public Jedis getMasterJedis() {
		throw new RedisException("redis manager do not support get master jedis!");
	}
	
	public boolean removeJedisPool(String server) {
		rwl.writeLock().lock();
		try {
			return availableServers.remove(server);
		} finally {
			rwl.writeLock().unlock();
		}
	}
	
	public void addJedisPool(String server) {
		rwl.writeLock().lock();
		try {
			if (availableServers.contains(server)) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("redis服务器[" + server + "]已存在可用列表中");
				}
			} else {
				availableServers.add(server);
			}
		} finally {
			rwl.writeLock().unlock();
		}
	}

	@Override
	protected void startInternal() {
		super.startInternal();
		String[] tmpServers = servers.split(",");
		if (tmpServers == null || tmpServers.length < 1) {
			throw new RedisException("redis服务器列表为空");
		}
		jedisPools = new ConcurrentHashMap<String, JedisPool>(tmpServers.length);
		availableServers = new ArrayList<String>(tmpServers.length);
		for (int i = 0; i < tmpServers.length; i++) {
			String[] tmpHostAndPorts = tmpServers[i].split(":");
			jedisPools.put(tmpServers[i], new JedisPool(getPoolConfig(), tmpHostAndPorts[0], Integer.parseInt(tmpHostAndPorts[1]), getTimeout(), getPassword(), getDatabase()));
		}

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("redis服务器[" + servers + "]启动完成");
		}
	}

	@Override
	protected void stopInternal() {
		rwl.writeLock().lock();
		try {
			if (availableServers != null) {
				availableServers.clear();
				availableServers = null;
			}
		} finally {
			rwl.writeLock().unlock();
		}
		
		for (Entry<String, JedisPool> entry : jedisPools.entrySet()) {
			String server = entry.getKey();
			JedisPool pool = entry.getValue();
			try {
				pool.close();
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]关闭出现异常", e);
				}
				continue;
			} finally {
				pool = null;
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("redis[" + server + "]关闭成功");
			}
		}
		jedisPools.clear();
		jedisPools = null;		
		super.stopInternal();
	}

	@Override
	public String getSentinels() {
		return null;
	}

	@Override
	public String getMasterName() {
		return null;
	}
	
}
