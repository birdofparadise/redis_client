package cn.mybop.redisclient.serialization;

import com.alibaba.fastjson.JSON;

import cn.mybop.redisclient.common.Constants;

public class JsonSerializable implements ISerializable {

	@Override
	public byte[] object2bytes(Object obj) {
		try {
			return JSON.toJSONString(obj).getBytes(Constants.Charset.UTF8);
		} catch (Exception e) {
			throw new SerializableException(e);
		}
	}

	@Override
	public Object bytes2object(byte[] bytes) {
		try {
			return JSON.parse(new String(bytes, Constants.Charset.UTF8));
		} catch (Exception e) {
			throw new SerializableException(e);
		}
	}

	@Override
	public <T> T bytes2object(byte[] bytes, Class<T> clazz) {
		try {
			return JSON.parseObject(new String(bytes, Constants.Charset.UTF8), clazz);
		} catch (Exception e) {
			throw new SerializableException(e);
		}
	}

}
