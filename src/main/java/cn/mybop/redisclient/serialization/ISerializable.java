package cn.mybop.redisclient.serialization;

public interface ISerializable {
	
	public byte[] object2bytes(Object obj);
	
	public Object bytes2object(byte[] bytes);
	
	public <T> T bytes2object(byte[] bytes, Class<T> clazz);

}
