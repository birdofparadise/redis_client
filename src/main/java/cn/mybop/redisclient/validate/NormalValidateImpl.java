package cn.mybop.redisclient.validate;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import cn.mybop.redisclient.serialization.ISerializable;
import redis.clients.jedis.Jedis;

public class NormalValidateImpl implements IValidate {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(NormalValidateImpl.class);

	@Override
	public boolean validate(Jedis jedis, ISerializable serializable, Properties props) {
		String key = props.getProperty(Constants.VALIDATE_KEY);
		String value = props.getProperty(Constants.VALIDATE_VALUE);
		String namespace = props.getProperty(Constants.SERVER_NAMESPACE);
		
		int compressThreshold = 0;
		String strCompressThreshold = props.getProperty(Constants.COMPRESS_THRESHOLD);
		if (Utils.isNotBlank(strCompressThreshold)) {
			compressThreshold = Integer.parseInt(strCompressThreshold);
		}
		try {
			byte[] bytes = null;
			try {
				bytes = Utils.mergeKey(namespace, key).getBytes(Constants.DEFAULT_CHARSET);
			} catch (UnsupportedEncodingException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("字符串转换字节失败", e);
				}
				return false;
			}
			byte[] rtnBytes = jedis.get(bytes);
			if (rtnBytes == null) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + jedis.getClient().getHost() + ":" + jedis.getClient().getPort() + "]中没有设置对应key的value");
				}
				return false;
			}			
			byte[] origBytes = Utils.getOrigBytes(rtnBytes, compressThreshold);			
			String rtn = serializable.bytes2object(origBytes, String.class);
			if (!value.equals(rtn)) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("redis[" + jedis.getClient().getHost() + ":" + jedis.getClient().getPort() + "]中key设置的value[" + rtn + "]不对");
				}
				return false;
			}
			return true;
		} catch (Exception e) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis[" + jedis.getClient().getHost() + ":" + jedis.getClient().getPort() + "]验证发生错误",  e);
			}
			return false;
		} 
	}

}
