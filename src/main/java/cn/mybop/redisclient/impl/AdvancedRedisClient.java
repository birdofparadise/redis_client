package cn.mybop.redisclient.impl;

import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisException;
import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.check.CheckTask;
import cn.mybop.redisclient.check.ICheckTask;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import cn.mybop.redisclient.serialization.ISerializable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

public abstract class AdvancedRedisClient extends AbstractRedisClient {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedRedisClient.class);
	
	private ScheduledExecutorService executor;
	
	public AdvancedRedisClient(Properties props) {
		super(props);
	}
	
	public RedisManager initManager() {
		Properties props = getProps();
		String servers = props.getProperty(Constants.SERVER_LIST);
		String loadBalancer = Constants.DEFAULT_LOADBALANCER;
		if (Utils.isNotBlank(props.getProperty(Constants.SERVER_LOADBALANCER))) {
			loadBalancer = props.getProperty(Constants.SERVER_LOADBALANCER);
		}
		JedisPoolConfig poolConfig = Utils.initPoolConfig(props);
		return new AdvancedRedisManager(servers, poolConfig, getTimeout(), getPassword(), getDatabase(), loadBalancer);
	}
	
	public ICheckTask initCheckTask() {
		Properties props = getProps();
		Class taskClazz = CheckTask.class;		
		if (Utils.isNotBlank(props.getProperty(Constants.CHECK_TASK))) {
			try {
				taskClazz = Class.forName(props.getProperty(Constants.CHECK_TASK));
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("加载redis checkTask类" + props.getProperty(Constants.CHECK_TASK) + "]失败 使用默认CheckTask类", e);
				}
			}
		}
		Class[] parameterTypes = {RedisManager.class, ISerializable.class, Properties.class};
		try {
			Constructor constructor = taskClazz.getConstructor(parameterTypes);
			Object[] parameters = {getManager(), getSerializable(), props};
			return (ICheckTask) constructor.newInstance(parameters);
		} catch (Exception e) {
			throw new RedisException("初始化CheckTask失败", e);
		}
	}
	
	@Override
	protected void startInternal() {
		super.startInternal();

		Properties props = getProps();
		
		//先执行一次checktask 增加可用服务列表
		ICheckTask checkTask = initCheckTask();
		checkTask.exec();

		//启动检查线程
		long initialDelay = Constants.DEFAULT_CHECK_SCHEDULE_INITIAL_DELAY;
		if (Utils.isNotBlank(props.getProperty(Constants.CHECK_SCHEDULE_INITIAL_DELAY))) {
			initialDelay = Long.parseLong(props.getProperty(Constants.CHECK_SCHEDULE_INITIAL_DELAY));
		}
		long delay = Constants.DEFAULT_CHECK_SCHEDULE_DELAY;
		if (Utils.isNotBlank(props.getProperty(Constants.CHECK_SCHEDULE_DELAY))) {
			delay = Long.parseLong(props.getProperty(Constants.CHECK_SCHEDULE_DELAY));
		}
		executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setDaemon(true);
				t.setName("redis-check-thread[" + getName() + "]");
				return t;
			}
			
		});
		executor.scheduleWithFixedDelay(checkTask, initialDelay, delay, TimeUnit.SECONDS);
	}

	@Override
	protected void stopInternal() {
		if (executor != null) {
			executor.shutdown();
			executor = null;
		}
		super.stopInternal();
	}

	@Override
	public void removeUnavailableServer(Jedis jedis) {
		if (jedis != null) {
			String server = Utils.getHostAndPort(jedis);
			if (getManager().removeJedisPool(server)) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis服务器[" + server + "]不可用 从可用列表中剔除");
				}
			}
		}
	}
	
}
