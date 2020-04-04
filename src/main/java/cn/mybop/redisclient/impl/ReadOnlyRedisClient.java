package cn.mybop.redisclient.impl;

import java.util.Map;
import java.util.Properties;

import cn.mybop.redisclient.RedisException;

public class ReadOnlyRedisClient extends AdvancedRedisClient {
	
	public ReadOnlyRedisClient(Properties props) {
		super(props);
	}
	
	@Override
	public String setBytes(String key, byte[] value) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public String set(String key, String value) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	public Long incr(String key) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	public Long incrBy(String key, long integer) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	public Long decr(String key) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	public Long decrBy(String key, long integer) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	public String set(String key, String value, String nxxx, String expx, int time) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public Long setnxBytes(String key, byte[] value) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public Long setnx(String key, String value) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public Long append(String key, String value) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public Long del(String key) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public String setObject(String key, Object value) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public Long setnxObject(String key, Object value) {
		throw new RedisException("read only redis client do not support write operation!");
	}

	@Override
	public String flushDB() {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public Long hset(String key, String hashKey, String hashVal) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public String hmset(String key, Map<String, String> map) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	public String rename(String oldkey, String newkey) {
		throw new RedisException("read only redis client do not support write operation!");
	}
	
	@Override
	protected void startInternal() {
		super.startInternal();
	}

	@Override
	protected void stopInternal() {
		super.stopInternal();
	}
	
}
