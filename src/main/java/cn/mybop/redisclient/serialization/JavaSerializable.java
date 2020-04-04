package cn.mybop.redisclient.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JavaSerializable implements ISerializable {

	@Override
	public byte[] object2bytes(Object obj) {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		try {
			new ObjectOutputStream(b).writeObject(obj);
		} catch (Exception e) {
			throw new SerializableException(e);
		}
	    return b.toByteArray();
	}

	@Override
	public Object bytes2object(byte[] bytes) {
		try {
			return new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
		} catch (Exception e) {
			throw new SerializableException(e);
		}
	}

	@Override
	public <T> T bytes2object(byte[] bytes, Class<T> clazz) {
		return (T) bytes2object(bytes);
	}

}
