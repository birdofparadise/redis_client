package cn.mybop.redisclient.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisException;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;


public class WriteAllReadAnyRedisClient extends AdvancedRedisClient {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WriteAllReadAnyRedisClient.class);
	
	private final static String FAILURE = "failure";
	
	private boolean throwExceptionWhenWriteError;
	
	public WriteAllReadAnyRedisClient(Properties props) {
		super(props);
		if (Utils.isNotBlank(props.getProperty(Constants.WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR))) {
			throwExceptionWhenWriteError = Boolean.parseBoolean(props.getProperty(Constants.WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR));
		} else {
			throwExceptionWhenWriteError = Constants.DEFAULT_WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR;
		}
	}
	
	@Override
	protected void startInternal() {
		super.startInternal();
	}

	@Override
	protected void stopInternal() {
		super.stopInternal();
	}
	
	public String setBytes(String key, byte[] value) {		
		List<String> servers = getManager().getAvailableServers();
		if (servers == null || servers.size() == 0) {
			throw new RedisException("无可用的redis服务器");
		}
		int maxByteSize = getMaxByteSize();
		String namespace = getNamespace();
		byte[] bytes = null;
		try {
			bytes = Utils.getCompressBytes(value, getCompressThreshold());
		} catch (IOException e) {
			throw new RedisException("压缩字节数组失败", e);
		}
		if (maxByteSize > 0 && bytes.length >= maxByteSize) {
			throw new RedisException("不能超过" + maxByteSize + "字节");
		}
		String btn = FAILURE;
		for (int index = 0; index < servers.size(); index++) {
			String server = servers.get(index);
			Jedis jedis = null;
			try {
				jedis = getManager().getJedis(server);
				btn = jedis.set(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET), bytes);
				if (!Constants.REPLY_CODE_OK.equals(btn)) {
					throw new RedisException("redis[" + server + "]应答" + btn);
				}
			} catch (UnsupportedEncodingException e) {
				throw new RedisException("redis[" + server + "]操作失败", e);
			} catch (JedisConnectionException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]操作失败", e);
				}
				//remove unavailable server
				removeUnavailableServer(jedis);
				if (throwExceptionWhenWriteError) {
					throw new RedisException("redis[" + server + "]操作失败", e);
				} else {
					continue;
				}
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]操作失败", e);
				}
				if (throwExceptionWhenWriteError) {
					throw new RedisException("redis[" + server + "]操作失败", e);
				} else {
					continue;
				}
			} finally {
				//close jedis
				closeJedis(jedis);
			}
		}
		return btn;
	}
	
	public Long setnxBytes(String key, byte[] value) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long incr(String key) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long decr(String key) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long decrBy(String key, long integer) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long incrBy(String key, long integer) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long append(String key, String value) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
		
	public String set(String key, String value) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public String set(String key, String value, String nxxx, String expx, int time) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long setnx(String key, String value) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	@Override
	public Long del(String key) {
		List<String> servers = getManager().getAvailableServers();
		if (servers == null || servers.size() == 0) {
			throw new RedisException("无可用的redis服务器");
		}
		Long btn = null;
		for (int index = 0; index < servers.size(); index++) {
			String server = servers.get(index);
			Jedis jedis = null;
			try {
				jedis = getManager().getJedis(server);
				btn = jedis.del(key);
			} catch (JedisConnectionException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]操作失败", e);
				}
				//remove unavailable server
				removeUnavailableServer(jedis);
				if (throwExceptionWhenWriteError) {
					throw new RedisException("redis[" + server + "]操作失败", e);
				} else {
					continue;
				}
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]操作失败", e);
				}
				if (throwExceptionWhenWriteError) {
					throw new RedisException("redis[" + server + "]操作失败", e);
				} else {
					continue;
				}
			} finally {
				//close jedis
				closeJedis(jedis);
			}
		}
		return btn;
	}
	
	@Override
	public String setObject(String key, Object value) {
		return setBytes(key, getSerializable().object2bytes(value));
	}
	
	@Override
	public Long setnxObject(String key, Object value) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
		
	@Override
	public String flushDB() {
		List<String> servers = getManager().getAvailableServers();
		if (servers == null || servers.size() == 0) {
			throw new RedisException("无可用的redis服务器");
		}
		String btn = FAILURE;
		for (int index = 0; index < servers.size(); index++) {
			String server = servers.get(index);
			Jedis jedis = null;
			try {
				jedis = getManager().getJedis(server);
				btn = jedis.flushDB();
				if (!Constants.REPLY_CODE_OK.equals(btn)) {
					throw new RedisException("redis[" + server + "]应答" + btn);
				}
			} catch (JedisConnectionException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]操作失败", e);
				}
				//remove unavailable server
				removeUnavailableServer(jedis);
				if (throwExceptionWhenWriteError) {
					throw new RedisException("redis[" + server + "]操作失败", e);
				} else {
					continue;
				}
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
				if (throwExceptionWhenWriteError) {
					throw new RedisException("redis[" + server + "]操作失败", e);
				} else {
					continue;
				}
			} finally {
				//close jedis
				closeJedis(jedis);
			}
		}
		return btn;
	}
	
	@Override
	public Long dbSize() {
		List<String> servers = getManager().getAvailableServers();
		if (servers == null || servers.size() == 0) {
			throw new RedisException("无可用的redis服务器");
		}
		Long rtn = 0l;
		for (int index = 0; index < servers.size(); index++) {
			String server = servers.get(index);
			Jedis jedis = null;
			try {
				jedis = getManager().getJedis(server);
				Long size = jedis.dbSize();
				if (index == 0) {
					rtn = size;
				} else {
					if (size != rtn) {
						throw new RedisException("redis节点间dbsize不一致");
					}
				}
			} catch (JedisConnectionException e) {
				//remove unavailable server
				removeUnavailableServer(jedis);
				throw new RedisException("redis[" + server + "]操作失败", e);
			} catch (Exception e) {
				throw new RedisException("redis[" + server + "]操作失败", e);
			} finally {
				//close jedis
				closeJedis(jedis);
			}
		}
		return rtn;
	}
	
	public String info() {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public String info(String section) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public String hget(String key,String hashKey) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public byte[] hgetBytes(String key,String hashKey) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Object hgetObject(String key,String hashKey) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Boolean hExists(String key,String hashKey) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Boolean exists(String key) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long hset(String key,String hashKey,String hashVal) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public String hmset(String key,Map<String,String> map) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}

	public String hmsetObject(String key,Map<String,Object> map) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Map<String,Object> hgetAllObject(String key){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Set<String> hkeys(String key){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public String rename(String oldkey,String newkey) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	public long zadd(String key,String value,double score){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}

	public LinkedHashSet<String> zrevrangebyscore(String key, String max, String min, int offset, int count){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}

	public long zrem(String key,String[] value){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public long expire(String key,int seconds){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long hsetBytes(String key,String hashKey,Object hashVal){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Set<String> getKeys(String pattern){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public Long zcount(String key, String max, String min){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
		
	public  <T> T  hgetGObject(String key,String hashKey,Class<T> clazz) {
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public long sadd(String key,String[] members){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public boolean sismember(String key,String value){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	

	public long hdel(String key,String field){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public long hdel(byte[] key,byte[] field){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	} 
	
	public Set<String> zrange(String key,long start,long end){
		throw new RedisException("write all read an redis client do temporarily  not support this operation!");
	}
	
}
