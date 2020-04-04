package cn.mybop.redisclient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import cn.mybop.redisclient.impl.ReadOnlyRedisClient;
import cn.mybop.redisclient.impl.SentinelClient;
import cn.mybop.redisclient.impl.SentinelRedisClient;
import cn.mybop.redisclient.impl.SharedClient;
import cn.mybop.redisclient.impl.WriteAllReadAnyRedisClient;

public class RedisClientFactory {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisClientFactory.class);
	
	private final static Map<String, IRedisClient> CLIENT_MAP = new ConcurrentHashMap<String, IRedisClient>();
	
	private final static Properties PROPS = new Properties();
	
	static {
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("redisclient.properties");
		if (is != null) {
			try {
				PROPS.load(is);
			} catch (IOException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("加载redisclient.properties失败", e);
				}
			} finally {
				if (is != null) {
					try {
						is.close();
						is = null;
					} catch (IOException e) {
						//do nothing
					}
				}
			}			
		} else {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("未发现redisclient.properties文件");
			}
		}
	}
	
	public static Properties getProperties(String name) {
		String prefix = name + ".";
		Properties props = new Properties();
		for (Entry<Object, Object> entry : PROPS.entrySet()) {
			String key = entry.getKey().toString();
			if (key.startsWith(prefix)) {
				props.put(key.substring(prefix.length()), entry.getValue());
			}
		}
		if (Utils.isBlank(props.getProperty(Constants.CLIENT_NAME))) {
			props.put(Constants.CLIENT_NAME, name);
		}
		
		return props;
	}
	
	/**
	 * 每次生成一个新的client
	 * @param name
	 * @return
	 */
	public static IRedisClient getClient(String name) {
		Properties props = getProperties(name);
		return getClient(props);
	}
	
	/**
	 * 每次生成一个新的client
	 * @param props
	 * @return
	 */
	public static IRedisClient getClient(Properties props) {
		String clientType = Constants.CLIENT_TYPE_DEFAULT;
		if (Utils.isNotBlank(props.getProperty(Constants.CLIENT_TYPE))) {
			clientType = props.getProperty(Constants.CLIENT_TYPE);
		} else {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("未定义" + Constants.CLIENT_TYPE + "属性 使用默认客户端");
			}
		}
		IRedisClient client = null;
		if (Constants.CLIENT_TYPE_DEFAULT.equalsIgnoreCase(clientType)) {
			client = new SentinelRedisClient(props);
		} else if (Constants.CLIENT_TYPE_SENTINEL.equalsIgnoreCase(clientType)) {
			client = new SentinelClient(props);
		} else if (Constants.CLIENT_TYPE_READONLY.equalsIgnoreCase(clientType)) {
			client = new ReadOnlyRedisClient(props);
		} else if (Constants.CLIENT_TYPE_WRITEALLREADANY.equalsIgnoreCase(clientType)) {
			client = new WriteAllReadAnyRedisClient(props);
		} else if (Constants.CLIENT_TYPE_SHARED.equalsIgnoreCase(clientType)) {
			client = new SharedClient(props);
		}
		
		if (client == null) {
			throw new RedisException("暂不支持其他类型客户端");
		}
		client.start();
		return client;
	}
	
	public static void closeClient(IRedisClient redisClient) {
		if (redisClient != null) {
			redisClient.stop();
			redisClient = null;
		}
	}
	
	/**
	 * 返回单例Client
	 * @param name
	 * @return
	 */
	public static IRedisClient getSingletonClient(String name) {
		IRedisClient client = CLIENT_MAP.get(name);
		if (client == null) {
			synchronized (CLIENT_MAP) {
				client = CLIENT_MAP.get(name);
				if (client == null) {
					client = getClient(name);
					CLIENT_MAP.put(name, client);
				}
			}
		}
		return client;
	}
	
}
