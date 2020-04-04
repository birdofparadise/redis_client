package cn.mybop.redisclient.validate;

import java.util.Properties;

import cn.mybop.redisclient.serialization.ISerializable;
import redis.clients.jedis.Jedis;

public class NoNeedValidateImpl implements IValidate {

	@Override
	public boolean validate(Jedis jedis, ISerializable serializable, Properties props) {
		return true;
	}

}
