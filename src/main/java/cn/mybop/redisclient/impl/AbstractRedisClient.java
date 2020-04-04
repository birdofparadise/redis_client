package cn.mybop.redisclient.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.IRedisClient;
import cn.mybop.redisclient.RedisException;
import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import cn.mybop.redisclient.lifecycle.LifecycleBase;
import cn.mybop.redisclient.serialization.ISerializable;
import cn.mybop.redisclient.serialization.SerializableFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class AbstractRedisClient extends LifecycleBase implements IRedisClient {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRedisClient.class);
	
	private Properties props;

	private String name;
	
	private String namespace;
	
	private int retryCount = 0;
	
	private long retrySleeptime = 0l;
	
	private int compressThreshold = 0;
	
	private int maxByteSize = 0;
	
	private ISerializable serializable;
	
	private RedisManager manager;
	
	private int timeout = Protocol.DEFAULT_TIMEOUT;
	
	private int database = Protocol.DEFAULT_DATABASE;
	
	private String password = null;

	public RedisManager getManager() {
		return manager;
	}

	public Properties getProps() {
		return props;
	}

	public String getName() {
		return name;
	}

	public String getNamespace() {
		return namespace;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public long getRetrySleeptime() {
		return retrySleeptime;
	}

	public ISerializable getSerializable() {
		return serializable;
	}

	public int getTimeout() {
		return timeout;
	}

	public int getDatabase() {
		return database;
	}

	public String getPassword() {
		return password;
	}
	
	public int getCompressThreshold() {
		return compressThreshold;
	}

	public int getMaxByteSize() {
		return maxByteSize;
	}

	public AbstractRedisClient(Properties props) {
		this.props = props;
	}
	
	@Override
	protected void startInternal() {
		//client name
		name = props.getProperty(Constants.CLIENT_NAME);
		if (Utils.isBlank(name)) {
			throw new RedisException(Constants.CLIENT_NAME + "参数未定义");
		}

		//重试次数
		String strRetryCount = props.getProperty(Constants.SERVER_RETRY_COUNT);
		if (Utils.isNotBlank(strRetryCount)) {
			retryCount = Integer.parseInt(strRetryCount);
		}
		//重试间隔时间
		String strRetrySleepTime = props.getProperty(Constants.SERVER_RETRY_SLEEPTIME);
		if (Utils.isNotBlank(strRetrySleepTime)) {
			retrySleeptime = Long.parseLong(strRetrySleepTime);
		}

		String strSerializable = props.getProperty(Constants.SERIALIZABLE);
		if (Utils.isNotBlank(strSerializable)) {
			serializable = SerializableFactory.getSerializable(strSerializable);
		} else {
			serializable = SerializableFactory.getSerializable(Constants.DEFAULT_SERIALIZABLE);
		}
		
		if (Utils.isNotBlank(props.getProperty(Constants.SERVER_TIMEOUT))) {
			timeout = Integer.parseInt(props.getProperty(Constants.SERVER_TIMEOUT));
		}
		
		if (Utils.isNotBlank(props.getProperty(Constants.SERVER_DATABASE))) {
			database = Integer.parseInt(props.getProperty(Constants.SERVER_DATABASE));
		}
		
		if (Utils.isNotBlank(props.getProperty(Constants.SERVER_PASSWORD))) {
			password = props.getProperty(Constants.SERVER_PASSWORD);
		}
		
		if (Utils.isNotBlank(props.getProperty(Constants.SERVER_NAMESPACE))) {
			namespace = props.getProperty(Constants.SERVER_NAMESPACE);
		}
		
		String strCompressThreshold = props.getProperty(Constants.COMPRESS_THRESHOLD);
		if (Utils.isNotBlank(strCompressThreshold)) {
			compressThreshold = Integer.parseInt(strCompressThreshold);
		}
		
		String strMaxByteSize = props.getProperty(Constants.MAX_BYTE_SIZE);
		if (Utils.isNotBlank(strMaxByteSize)) {
			maxByteSize = Integer.parseInt(strMaxByteSize);
		}
		
		manager = initManager();
		if (manager != null) {
			manager.start();
		}
	}
	
	@Override
	protected void stopInternal() {
		if (manager != null) {
			manager.stop();
			manager = null;
		}
	}
	
	/**
	 * remove unavailable server
	 * @param jedis
	 */
	public abstract void removeUnavailableServer(Jedis jedis);
	
	/**
	 * close jedis
	 * @param jedis
	 */
	public void closeJedis(Jedis jedis) {
		if (jedis != null) {
			try {
				jedis.close();
			} catch (Exception e) {
				//do nothing
			} finally {
				jedis = null;
			}
		}
	}
	
	public byte[] getBytes(String key) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			byte[] bytes = jedis.get(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET));
			return Utils.getOrigBytes(bytes, compressThreshold);
		} catch (UnsupportedEncodingException e) {
			throw new RedisException("redis操作失败", e);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				byte[] bytes = retryJedis.get(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET));
				return Utils.getOrigBytes(bytes, compressThreshold);
			} catch (UnsupportedEncodingException e) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e);
			} catch (JedisConnectionException e) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e);
				}
			} catch (Exception e) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String setBytes(String key, byte[] value) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			byte[] bytes = Utils.getCompressBytes(value, compressThreshold);
			if (maxByteSize > 0 && value.length >= maxByteSize) {
				throw new RedisException("不能超过" + maxByteSize + "字节");
			}
			return jedis.set(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET), bytes);
		} catch (UnsupportedEncodingException e) {
			throw new RedisException("redis操作失败", e);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
				
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				byte[] bytes = Utils.getCompressBytes(value, compressThreshold);
				if (maxByteSize > 0 && value.length >= maxByteSize) {
					throw new RedisException("不能超过" + maxByteSize + "字节");
				}
				return retryJedis.set(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET), bytes);
			} catch (UnsupportedEncodingException e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long setnxBytes(String key, byte[] value) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			byte[] bytes = Utils.getCompressBytes(value, compressThreshold);
			if (maxByteSize > 0 && value.length >= maxByteSize) {
				throw new RedisException("不能超过" + maxByteSize + "字节");
			}
			return jedis.setnx(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET), bytes);
		} catch (UnsupportedEncodingException e) {
			throw new RedisException("redis操作失败", e);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				byte[] bytes = Utils.getCompressBytes(value, compressThreshold);
				if (maxByteSize > 0 && value.length >= maxByteSize) {
					throw new RedisException("不能超过" + maxByteSize + "字节");
				}
				return retryJedis.setnx(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET), bytes);
			} catch (UnsupportedEncodingException e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long incr(String key) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.incr(Utils.mergeKey(namespace, key));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
				
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.incr(Utils.mergeKey(namespace, key));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long decr(String key) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.decr(Utils.mergeKey(namespace, key));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}		
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
				
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.decr(Utils.mergeKey(namespace, key));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long decrBy(String key, long integer) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.decrBy(Utils.mergeKey(namespace, key), integer);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.decrBy(Utils.mergeKey(namespace, key), integer);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long incrBy(String key, long integer) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.incrBy(Utils.mergeKey(namespace, key), integer);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}	
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
				
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.incrBy(Utils.mergeKey(namespace, key), integer);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long append(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.append(Utils.mergeKey(namespace, key), value);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.append(Utils.mergeKey(namespace, key), value);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String get(String key) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.get(Utils.mergeKey(namespace, key));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.get(Utils.mergeKey(namespace, key));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(jedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String set(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.set(Utils.mergeKey(namespace, key), value);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}	
		}  catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}		
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.set(Utils.mergeKey(namespace, key), value);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String set(String key, String value, String nxxx, String expx, int time) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.set(Utils.mergeKey(namespace, key), value, nxxx, expx, time);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		}  catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.set(Utils.mergeKey(namespace, key), value, nxxx, expx, time);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long setnx(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.setnx(Utils.mergeKey(namespace, key), value);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		}  catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.setnx(Utils.mergeKey(namespace, key), value);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	@Override
	public Long del(String key) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.del(Utils.mergeKey(namespace, key));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		}  catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.del(Utils.mergeKey(namespace, key));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	@Override
	public Object getObject(String key) {
		byte[] bytes = getBytes(key);
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		return serializable.bytes2object(bytes);
	}
	
	@Override
	public String setObject(String key, Object value) {
		return setBytes(key, serializable.object2bytes(value));
	}
	
	@Override
	public Long setnxObject(String key, Object value) {
		return setnxBytes(key, serializable.object2bytes(value));
	}
	
	@Override
	public Map<String, byte[]> getBytes(String[] keys) {		
		Jedis jedis = null;
		Map<String, byte[]> rtn = new HashMap<String, byte[]>();
		int idx = 0;
		try {
			jedis = manager.getJedis();
			Pipeline pipe = jedis.pipelined();	
			Map<String, Response<byte[]>> responseMap = new HashMap<String, Response<byte[]>>();
			for (; idx < keys.length; idx++) {
				responseMap.put(keys[idx], pipe.get(Utils.mergeKey(namespace, keys[idx]).getBytes(Constants.DEFAULT_CHARSET)));
			}
			pipe.sync();
			for (Entry<String, Response<byte[]>> entry : responseMap.entrySet()) {
				if (entry.getValue().get() != null && entry.getValue().get().length > 0) {
					byte[] bytes = Utils.getOrigBytes(entry.getValue().get(), compressThreshold);
					rtn.put(entry.getKey(), bytes);
				}
			}
			return rtn;
		} catch (UnsupportedEncodingException e) {
			throw new RedisException("redis操作失败", e);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}	
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				Pipeline pipe = retryJedis.pipelined();	
				Map<String, Response<byte[]>> responseMap = new HashMap<String, Response<byte[]>>();
				for (; idx < keys.length; idx++) {
					responseMap.put(keys[idx], pipe.get(Utils.mergeKey(namespace, keys[idx]).getBytes(Constants.DEFAULT_CHARSET)));
				}
				pipe.sync();
				for (Entry<String, Response<byte[]>> entry : responseMap.entrySet()) {
					if (entry.getValue().get() != null && entry.getValue().get().length > 0) {
						byte[] bytes = Utils.getOrigBytes(entry.getValue().get(), compressThreshold);
						rtn.put(entry.getKey(), bytes);
					}
				}
				return rtn;
			} catch (UnsupportedEncodingException e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}

	@Override
	public Map<String, Object> getObject(String[] keys) {
		Map<String, byte[]> map = getBytes(keys);
		Map<String, Object> rtn = new HashMap<String, Object>();
		if (map == null || map.size() == 0) {
			return rtn;
		}
		for (Entry<String, byte[]> entry : map.entrySet()) {
			byte[] bytes = entry.getValue();
			if (bytes == null || bytes.length == 0) {
				continue;
			}
			rtn.put(entry.getKey(), serializable.bytes2object(bytes));
		}
		return rtn;
	}

	@Override
	public Map<String, String> get(String[] keys) {
		Jedis jedis = null;
		Map<String, String> rtn = new HashMap<String, String>();
		int idx = 0;
		try {
			jedis = manager.getJedis();
			Pipeline pipe = jedis.pipelined();	
			Map<String, Response<String>> responseMap = new HashMap<String, Response<String>>();
			for (; idx < keys.length; idx++) {
				responseMap.put(keys[idx], pipe.get(Utils.mergeKey(namespace, keys[idx])));
			}
			pipe.sync();
			for (Entry<String, Response<String>> entry : responseMap.entrySet()) {
				if (entry.getValue().get() != null) {
					rtn.put(entry.getKey(), entry.getValue().get());
				}
			}
			return rtn;
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				Pipeline pipe = retryJedis.pipelined();	
				Map<String, Response<String>> responseMap = new HashMap<String, Response<String>>();
				for (; idx < keys.length; idx++) {
					responseMap.put(keys[idx], pipe.get(Utils.mergeKey(namespace, keys[idx])));
				}
				pipe.sync();
				for (Entry<String, Response<String>> entry : responseMap.entrySet()) {
					if (entry.getValue().get() != null) {
						rtn.put(entry.getKey(), entry.getValue().get());
					}
				}
				return rtn;
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	@Override
	public String flushDB() {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.flushDB();
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		}  catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.flushDB();
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	@Override
	public Long dbSize() {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.dbSize();
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}				
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.dbSize();
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String info() {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.info();
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		}  catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.info();
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String info(String section) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.info(section);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		}  catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.info(section);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String hget(String key,String hashKey) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.hget(Utils.mergeKey(namespace, key), hashKey);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.hget(Utils.mergeKey(namespace, key), hashKey);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public byte[] hgetBytes(String key,String hashKey) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.hget(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET),hashKey.getBytes(Constants.DEFAULT_CHARSET));
		} catch (UnsupportedEncodingException e) {
			throw new RedisException("redis操作失败", e);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.hget(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET),hashKey.getBytes(Constants.DEFAULT_CHARSET));
			} catch (UnsupportedEncodingException e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Object hgetObject(String key,String hashKey) {
		byte[] bytes = hgetBytes(key,hashKey);
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		return serializable.bytes2object(bytes);
	}
	
	public Boolean hExists(String key,String hashKey) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.hexists(Utils.mergeKey(namespace, key), hashKey);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}				
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.hexists(Utils.mergeKey(namespace, key), hashKey);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Boolean exists(String key) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.exists(Utils.mergeKey(namespace, key));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.exists(Utils.mergeKey(namespace, key));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long hset(String key,String hashKey,String hashVal) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.hset(Utils.mergeKey(namespace, key), hashKey,hashVal);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.hset(Utils.mergeKey(namespace, key), hashKey,hashVal);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);	
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String hmset(String key,Map<String,String> map) {
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.hmset(Utils.mergeKey(namespace, key),map);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.hmset(Utils.mergeKey(namespace, key),map);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}

	public String hmsetObject(String key,Map<String,Object> map) {		
		if(map == null || map.size()<=0) {
			return null;
		}
		Map<byte[], byte[]> hashes = new LinkedHashMap(map.size());
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			byte[] bytes = null;
			try{
				bytes = Utils.getCompressBytes(serializable.object2bytes(entry.getValue()), compressThreshold);
			} catch (IOException e) {
				throw new RedisException("redis操作失败", e);
			}
			try {
				hashes.put(entry.getKey().getBytes(Constants.DEFAULT_CHARSET), bytes);
			} catch (UnsupportedEncodingException e) {
				throw new RedisException("redis操作失败", e);
			}
		}
		
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.hmset(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET),hashes);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.hmset(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET),hashes);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Map<String,Object> hgetAllObject(String key){
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			Map<byte[],byte[]> hashes = jedis.hgetAll(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET));
			if(hashes == null || hashes.isEmpty()) {
				return null;
			} else {
				Map retMap = new HashMap(hashes.size());
				for (Map.Entry<byte[],byte[]> entry : hashes.entrySet()) {
					byte[] bytes = Utils.getOrigBytes(entry.getValue(), compressThreshold);
					retMap.put(new String(entry.getKey(),Constants.DEFAULT_CHARSET), bytes);
				}
				return retMap;
			}
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}	
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				Map<byte[],byte[]> hashes = retryJedis.hgetAll(Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET));
				if(hashes == null || hashes.size()<=0) {
					return null;
				}else {
					Map retMap = new HashMap(hashes.size());
					for (Map.Entry<byte[],byte[]> entry : hashes.entrySet()) {
						byte[] bytes = Utils.getOrigBytes(entry.getValue(), compressThreshold);
						retMap.put(new String(entry.getKey(),Constants.DEFAULT_CHARSET), bytes);
					}
					return retMap;
				}
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Set<String> hkeys(String key){
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.hkeys(Utils.mergeKey(namespace, key));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.hkeys(Utils.mergeKey(namespace, key));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public String rename(String oldkey,String newkey) {
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.rename(Utils.mergeKey(namespace, oldkey), Utils.mergeKey(namespace, newkey));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			//close jedis
			closeJedis(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.rename(Utils.mergeKey(namespace, oldkey), Utils.mergeKey(namespace, newkey));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	public long zadd(String key,String value,double score){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.zadd(Utils.mergeKey(namespace, key), score,value);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}	
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.zadd(Utils.mergeKey(namespace, key), score,value);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}

	public LinkedHashSet<String> zrevrangebyscore(String key, String max, String min, int offset, int count){
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return (LinkedHashSet<String>)jedis.zrevrangeByScore(Utils.mergeKey(namespace, key), max, min, offset, count);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return (LinkedHashSet<String>)retryJedis.zrevrangeByScore(Utils.mergeKey(namespace, key), max, min, offset, count);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}

	public long zrem(String key,String[] value){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.zrem(Utils.mergeKey(namespace, key), value);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			//close jedis
			closeJedis(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.zrem(Utils.mergeKey(namespace, key), value);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public long expire(String key,int seconds){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.expire(Utils.mergeKey(namespace, key), seconds);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retyJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retyJedis = manager.getMasterJedis();
				return retyJedis.expire(Utils.mergeKey(namespace, key), seconds);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retyJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retyJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long hsetBytes(String key,String hashKey,Object hashVal){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.hset(Utils.mergeKey(namespace, key).getBytes(), hashKey.getBytes(), serializable.object2bytes(hashVal));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.hset(Utils.mergeKey(namespace, key).getBytes(), hashKey.getBytes(), serializable.object2bytes(hashVal));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Set<String> getKeys(String pattern){
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.keys(Utils.mergeKey(namespace, pattern));
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.keys(Utils.mergeKey(namespace, pattern));
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public Long zcount(String key, String max, String min){
		Jedis jedis = null;
		try {
			jedis = manager.getJedis();
			return jedis.zcount(Utils.mergeKey(namespace, key),max,min);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getJedis();
				return retryJedis.zcount(Utils.mergeKey(namespace, key),max,min);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
		
	public  <T> T  hgetGObject(String key,String hashKey,Class<T> clazz) {
			byte[] bytes = hgetBytes(key,hashKey);
			if (bytes == null || bytes.length == 0) {
				return null;
			}
			return serializable.bytes2object(bytes, clazz);
	}
	
	public long sadd(String key,String[] members){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.sadd(Utils.mergeKey(namespace, key), members);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.sadd(Utils.mergeKey(namespace, key), members);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public boolean sismember(String key,String value){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.sismember(key, value);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.sismember(key, value);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	

	public long hdel(String key,String field){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.hdel(key, field);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.hdel(key, field);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public long hdel(byte[] key,byte[] field){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.hdel(key, field);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return retryJedis.hdel(key, field);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
	
	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.zscan(key, cursor, params);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return  retryJedis.zscan(key, cursor, params);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	} 
	
	public Set<String> zrange(String key,long start,long end){
		Jedis jedis = null;
		try {
			jedis = manager.getMasterJedis();
			return jedis.zrange(key, start, end);
		} catch (JedisConnectionException e) {
			//remove unavailable server
			removeUnavailableServer(jedis);
			if (retryCount == 0) {
				throw new RedisException("redis操作失败", e);
			} else {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis操作失败", e);
				}
			}			
		} catch (Exception e) {
			throw new RedisException("redis操作失败", e);
		} finally {
			//close jedis
			closeJedis(jedis);
		}
		
		for (int i = 0; i < retryCount; i++) {
			Jedis retryJedis = null;
			try {
				if (retrySleeptime > 0) {
					Thread.sleep(retrySleeptime);
				}
				retryJedis = manager.getMasterJedis();
				return  retryJedis.zrange(key,start,end);
			} catch (JedisConnectionException e1) {
				//remove unavailable server
				removeUnavailableServer(retryJedis);
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis第" + (i + 1) + "次重试失败", e1);
				}
			} catch (Exception e1) {
				throw new RedisException("redis第" + (i + 1) + "次重试失败", e1);
			} finally {
				//close jedis
				closeJedis(retryJedis);
			}
		}
		throw new RedisException("redis达到最大重试次数抛出异常");
	}
}
