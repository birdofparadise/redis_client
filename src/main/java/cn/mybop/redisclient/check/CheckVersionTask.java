package cn.mybop.redisclient.check;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import cn.mybop.redisclient.serialization.ISerializable;
import redis.clients.jedis.Jedis;

public class CheckVersionTask extends AbstractCheckTask {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CheckVersionTask.class);
	
	private String versionKey;
	
	public CheckVersionTask(RedisManager manager, ISerializable serializable, Properties props) {
		super(manager, serializable, props);
		if (Utils.isNotBlank(props.getProperty(Constants.VERSION_KEY))) {
			versionKey = props.getProperty(Constants.VERSION_KEY);
		} else {
			versionKey = Constants.DEFAULT_VERSION_KEY;
		}
	}
	
	@Override
	public void exec() {
		RedisManager manager = getManager();
		
		List<String> availableServers = manager.getAvailableServers();
		Long maxAvailableVersion = 0l;
		if (availableServers != null && availableServers.size() > 0) {
			Map<String, Long> versionMap = new HashMap<String, Long>(availableServers.size());
			for (int i = 0; i < availableServers.size(); i++) {
				String server = availableServers.get(i);
				Jedis jedis = null;
				try {
					jedis = manager.getJedis(server);
					if (checkPing(jedis) && checkValidate(jedis)) {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("redis[" + server + "]检查通过");
						}
						Long version = getVersion(jedis);
						if (maxAvailableVersion.longValue() < version.longValue()) {
							maxAvailableVersion = version;
						}
						versionMap.put(server, version);
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
			
			for (Entry<String, Long> entry : versionMap.entrySet()) {
				String server = entry.getKey();
				if (entry.getValue().longValue() != maxAvailableVersion.longValue()) {
					if (LOGGER.isErrorEnabled()) {
						LOGGER.error("redis[" + server + "]版本信息检查不通过 从可用列表中移除");
					}
					manager.removeJedisPool(server);
				}
			}
		}
		
		availableServers = manager.getAvailableServers();
		String[] allServers = manager.getServers().split(",");
		if (allServers.length == availableServers.size()) {
			return;
		}
		
		Long maxNewVersion = 0l;
		Map<String, Long> versionMap = new HashMap<String, Long>();
		for (int i = 0; i < allServers.length; i++) {
			String server = allServers[i];
			if (manager.isAvailableServer(server)) {
				continue;
			}			
			Jedis jedis = null;
			try {
				jedis = manager.getJedis(server);
				if (checkPing(jedis) && checkValidate(jedis)) {
					Long version = getVersion(jedis);
					if (maxNewVersion.longValue() < version.longValue()) {
						maxNewVersion = version;
					}
					versionMap.put(server, version);					
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
		if (maxAvailableVersion.longValue() == 0l) {
			for (Entry<String, Long> entry : versionMap.entrySet()) {
				String server = entry.getKey();
				if (entry.getValue().longValue() == maxNewVersion.longValue()) {
					if (LOGGER.isErrorEnabled()) {
						LOGGER.error("redis[" + server + "]版本信息检查通过 加入可用列表");
					}
					manager.addJedisPool(server);
				} else {
					if (LOGGER.isErrorEnabled()) {
						LOGGER.error("redis[" + server + "]版本信息检查不通过 无法加入可用列表");
					}
				}
			}
		} else {
			for (Entry<String, Long> entry : versionMap.entrySet()) {
				String server = entry.getKey();
				if (entry.getValue().longValue() == maxAvailableVersion.longValue()) {
					if (LOGGER.isErrorEnabled()) {
						LOGGER.error("redis[" + server + "]版本信息检查通过 加入可用列表");
					}
					manager.addJedisPool(server);
				} else {
					if (LOGGER.isErrorEnabled()) {
						LOGGER.error("redis[" + server + "]版本信息检查不通过 无法加入可用列表");
					}
				}
			}
		}
	}
	
	private Long getVersion(Jedis jedis) {
		Properties props = getProps();
		ISerializable serializable = getSerializable();
		
		String namespace = props.getProperty(Constants.SERVER_NAMESPACE);
		int compressThreshold = 0;
		String strCompressThreshold = props.getProperty(Constants.COMPRESS_THRESHOLD);
		if (Utils.isNotBlank(strCompressThreshold)) {
			compressThreshold = Integer.parseInt(strCompressThreshold);
		}
		try {
			byte[] bytes = null;
			try {
				bytes = Utils.mergeKey(namespace, versionKey).getBytes(Constants.DEFAULT_CHARSET);
			} catch (UnsupportedEncodingException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("字符串转换字节失败", e);
				}
				return -1l;
			}
			byte[] rtnBytes = jedis.get(bytes);
			if (rtnBytes == null) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + Utils.getHostAndPort(jedis) + "]中没有设置对应版本信息");
				}
				return -1l;
			}			
			byte[] origBytes = Utils.getOrigBytes(rtnBytes, compressThreshold);			
			String rtn = serializable.bytes2object(origBytes, String.class);
			return Long.valueOf(rtn);
		} catch (Exception e) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis[" + Utils.getHostAndPort(jedis) + "]获取版本信息发生错误",  e);
			}
			return -1l;
		}
	}

}
