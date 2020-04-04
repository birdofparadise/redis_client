package cn.mybop.redisclient.check;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.serialization.ISerializable;
import redis.clients.jedis.Jedis;

public class CheckTask extends AbstractCheckTask {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CheckTask.class);
	
	public CheckTask(RedisManager manager, ISerializable serializable, Properties props) {
		super(manager, serializable, props);
	}
	
	@Override
	public void exec() {
		RedisManager manager = getManager();
		
		List<String> availableServers = manager.getAvailableServers();
		if (availableServers != null && availableServers.size() > 0) {
			for (int i = 0; i < availableServers.size(); i++) {
				String server = availableServers.get(i);
				Jedis jedis = null;
				try {
					jedis = manager.getJedis(server);
					if (checkPing(jedis) && checkValidate(jedis)) {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("redis[" + server + "]检查通过");
						}
						continue;
					}
					if (LOGGER.isErrorEnabled()) {
						LOGGER.error("redis[" + server + "]检查不通过 从可用列表中移除");
					}
					manager.removeJedisPool(server);
				} catch (Exception e) {
					if (LOGGER.isErrorEnabled()) {
						LOGGER.error("redis[" + server + "]检查失败 从可用列表中移除", e);
					}
					manager.removeJedisPool(server);
				} finally {
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
			}
		}
		
		availableServers = manager.getAvailableServers();
		String[] allServers = manager.getServers().split(",");
		if (allServers.length == availableServers.size()) {
			return;
		}
		
		for (int i = 0; i < allServers.length; i++) {
			String server = allServers[i];
			if (manager.isAvailableServer(server)) {
				continue;
			}
			Jedis jedis = null;
			try {
				jedis = manager.getJedis(server);
				if (checkPing(jedis) && checkValidate(jedis)) {
					if (LOGGER.isInfoEnabled()) {
						LOGGER.info("redis[" + server + "]检查通过 加入可用列表");
					}
					manager.addJedisPool(server);
					continue;
				}
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]检查不通过");
				}
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + server + "]检查失败", e);
				}
			} finally {
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
		}
	}
	
	

}
