package cn.mybop.redisclient.validate;

import java.util.Properties;

import cn.mybop.redisclient.serialization.ISerializable;
import redis.clients.jedis.Jedis;

public interface IValidate {
	
	public boolean validate(Jedis jedis, ISerializable serializable, Properties props);

}
