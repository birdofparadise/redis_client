package cn.mybop.redisclient;

import java.sql.Timestamp;
import java.util.List;

import cn.mybop.redisclient.lifecycle.Lifecycle;
import redis.clients.jedis.Jedis;

public interface RedisManager extends Lifecycle {
		
	public String getServers();
	
	public String getSentinels();
	
	public String getMasterName();
	
	public List<String> getAvailableServers();
	
	public Jedis getJedis(String server);
	
	public Jedis getJedis();
	
	public Jedis getMasterJedis();
		
	public boolean removeJedisPool(String server);
	
	public void addJedisPool(String server);
	
	public boolean isAvailableServer(String server);
	
	public Timestamp getStartTime();

}
