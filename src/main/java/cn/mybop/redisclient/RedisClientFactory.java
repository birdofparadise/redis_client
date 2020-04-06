package cn.mybop.redisclient;

import java.util.List;
import java.util.Properties;

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
		}
		
		if (client == null) {
			throw new RedisException("暂不支持其他类型客户端");
		}
		client.start();
		return client;
	}
	
	public static IRedisClient getShareClient(Properties props, List<IRedisClient> clients) {
		IRedisClient client = new SharedClient(props, clients);
		client.start();
		return client;
	}
	
	public static void closeClient(IRedisClient redisClient) {
		if (redisClient != null) {
			redisClient.stop();
			redisClient = null;
		}
	}
	
}
