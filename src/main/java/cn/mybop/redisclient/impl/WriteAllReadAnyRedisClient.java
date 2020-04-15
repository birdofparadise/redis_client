package cn.mybop.redisclient.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
		
	private ExecutorService executor;
	
	private boolean throwExceptionWhenWriteError;
	
	private boolean asyncWrite;
	
	public WriteAllReadAnyRedisClient(Properties props) {
		super(props);
		if (Utils.isNotBlank(props.getProperty(Constants.WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR))) {
			throwExceptionWhenWriteError = Boolean.parseBoolean(props.getProperty(Constants.WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR));
		} else {
			throwExceptionWhenWriteError = Constants.DEFAULT_WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR;
		}
		
		if (Utils.isNotBlank(props.getProperty(Constants.WRITEALL_ASYNC_WRITE))) {
			asyncWrite = Boolean.parseBoolean(props.getProperty(Constants.WRITEALL_ASYNC_WRITE));
		} else {
			asyncWrite = Constants.DEFAULT_WRITEALL_ASYNC_WRITE;
		}
		
		if (Utils.isNotBlank(props.getProperty(Constants.WRITEALL_THREAD_POOL))) {
			String threadPool = props.getProperty(Constants.WRITEALL_THREAD_POOL);
			String[] tmpArr = threadPool.split(";");
			if (tmpArr.length != 4) {
				throw new RedisException("线程池[" + threadPool + "]参数个数不等于4");
			}
			executor = new ThreadPoolExecutor(Integer.parseInt(tmpArr[0]), Integer.parseInt(tmpArr[1]),
					Integer.parseInt(tmpArr[3]), TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>(Integer.parseInt(tmpArr[2])),
					new ThreadFactory() {

						@Override
						public Thread newThread(Runnable r) {
							Thread t = new Thread(r);
							t.setDaemon(true);
							t.setName("redis-exec-thread[" + getName() + "]");
							return t;
						}
						
					});
		}
	}
	
	@Override
	protected void startInternal() {
		super.startInternal();
	}

	@Override
	protected void stopInternal() {
		if (executor != null) {
			executor.shutdown();
			executor = null;
		}
		super.stopInternal();
	}
	
	public String setBytes(String key, byte[] value) {		
		List<String> servers = getManager().getAvailableServers();
		if (servers == null || servers.size() == 0) {
			throw new RedisException("无可用的redis服务器");
		}
		int compressThreshold = getCompressThreshold();
		int maxByteSize = getMaxByteSize();
		String namespace = getNamespace();
		final byte[] keyBytes;
		try {
			keyBytes = Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET);
		} catch (UnsupportedEncodingException e) {
			throw new RedisException("key[" + key + "]转字符码失败", e);
		}
		final byte[] valueByes;
		try {
			valueByes = Utils.getCompressBytes(value, compressThreshold);
		} catch (IOException e) {
			throw new RedisException("压缩字节数组失败", e);
		}
		if (maxByteSize > 0 && valueByes.length >= maxByteSize) {
			throw new RedisException("不能超过" + maxByteSize + "字节");
		}
		String btn = Constants.REPLY_CODE_OK;
		Map<String, Future<String>> futures = null;
		for (int index = 0; index < servers.size(); index++) {
			final String server = servers.get(index);
			if (executor == null) {
				btn = _setBytes(server, keyBytes, valueByes);
			} else {
				if (futures == null) {
					futures = new HashMap<String, Future<String>>(servers.size());
				}
				Future<String> f = executor.submit(new Callable<String>() {
					@Override
					public String call() throws Exception {						
						return _setBytes(server, keyBytes, valueByes);
					}
				});
				futures.put(server, f);
			}
		}
		if (!asyncWrite && futures != null) {
			for (Entry<String, Future<String>> entry : futures.entrySet()) {
				Future<String> f = entry.getValue();
				try {
					btn = f.get();
				} catch (Exception e) {
					throw new RedisException("redis[" + entry.getKey() + "]操作失败", e);
				}
			}
		}
		return btn;
	}
	
	private String _setBytes(String server, byte[] keyBytes, byte[] valueByes) {
		String btn = Constants.REPLY_CODE_OK;
		Jedis jedis = null;
		try {
			jedis = getManager().getJedis(server);
			btn = jedis.set(keyBytes, valueByes);
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
			}
		} catch (Exception e) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis[" + server + "]操作失败", e);
			}
			if (throwExceptionWhenWriteError) {
				throw new RedisException("redis[" + server + "]操作失败", e);
			}
		} finally {
			//close jedis
			closeJedis(jedis);			
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
		Map<String, Future<Long>> futures = null;
		for (int index = 0; index < servers.size(); index++) {
			final String server = servers.get(index);
			if (executor == null) {
				btn = _del(server, key);
			} else {
				if (futures == null) {
					futures = new HashMap<String, Future<Long>>(servers.size());
				}
				Future<Long> f = executor.submit(new Callable<Long>() {
					@Override
					public Long call() throws Exception {						
						return _del(server, key);
					}
				});
				futures.put(server, f);
			}
		}
		if (!asyncWrite && futures != null) {
			for (Entry<String, Future<Long>> entry : futures.entrySet()) {
				Future<Long> f = entry.getValue();
				try {
					btn = f.get();
				} catch (Exception e) {
					throw new RedisException("redis[" + entry.getKey() + "]操作失败", e);
				}
			}
		}
		return btn;
	}
	
	private Long _del(String server, String key) {
		Long btn = null;
		Jedis jedis = null;
		try {
			jedis = getManager().getJedis(server);
			return jedis.del(key);
		} catch (JedisConnectionException e) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis[" + server + "]操作失败", e);
			}
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (throwExceptionWhenWriteError) {
				throw new RedisException("redis[" + server + "]操作失败", e);
			}
		} catch (Exception e) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis[" + server + "]操作失败", e);
			}
			if (throwExceptionWhenWriteError) {
				throw new RedisException("redis[" + server + "]操作失败", e);
			}
		} finally {
			//close jedis
			closeJedis(jedis);
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
		String btn = Constants.REPLY_CODE_OK;
		Map<String, Future<String>> futures = null;
		for (int index = 0; index < servers.size(); index++) {
			final String server = servers.get(index);
			if (executor == null) {
				btn = _flushDB(server);
			} else {
				if (futures == null) {
					futures = new HashMap<String, Future<String>>(servers.size());
				}
				Future<String> f = executor.submit(new Callable<String>() {
					@Override
					public String call() throws Exception {						
						return _flushDB(server);
					}
				});
				futures.put(server, f);
			}
		}
		if (!asyncWrite && futures != null) {
			for (Entry<String, Future<String>> entry : futures.entrySet()) {
				Future<String> f = entry.getValue();
				try {
					btn = f.get();
				} catch (Exception e) {
					throw new RedisException("redis[" + entry.getKey() + "]操作失败", e);
				}
			}
		}
		return btn;
	}
	
	private String _flushDB(String server) {
		String btn = Constants.REPLY_CODE_OK;
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
			}
		} catch (Exception e) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis操作失败", e);
			}
			if (throwExceptionWhenWriteError) {
				throw new RedisException("redis[" + server + "]操作失败", e);
			}
		} finally {
			//close jedis
			closeJedis(jedis);
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
		Map<String, Future<Long>> futures = null;
		for (int index = 0; index < servers.size(); index++) {
			final String server = servers.get(index);
			if (executor == null) {
				Long size = _dbSize(server);
				if (index == 0) {
					rtn = size;
				} else {
					if (size.longValue() != rtn.longValue()) {
						throw new RedisException("redis节点间dbsize不一致");
					}
				}
			} else {
				if (futures == null) {
					futures = new HashMap<String, Future<Long>>(servers.size());
				}
				Future<Long> f = executor.submit(new Callable<Long>() {
					@Override
					public Long call() throws Exception {						
						return _dbSize(server);
					}
				});
				futures.put(server, f);
			}
		}
		if (!asyncWrite && futures != null) {
			int i = 0;
			for (Entry<String, Future<Long>> entry : futures.entrySet()) {
				Future<Long> f = entry.getValue();
				try {
					Long size = f.get();
					if (i++ == 0) {
						rtn = size;
					} else {
						if (size != rtn) {
							throw new RedisException("redis节点间dbsize不一致");
						}
					}
				} catch (Exception e) {
					throw new RedisException("redis[" + entry.getKey() + "]操作失败", e);
				}
			}
		}
		return rtn;
	}
	
	private Long _dbSize(String server) {
		Jedis jedis = null;
		try {
			jedis = getManager().getJedis(server);
			return jedis.dbSize();
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
