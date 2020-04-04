package cn.mybop.redisclient;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import cn.mybop.redisclient.lifecycle.Lifecycle;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

public interface IRedisClient extends Lifecycle {
	
	public String getName();
	
	public String getNamespace();
	
	public RedisManager initManager();
	
	public RedisManager getManager();
	
	public String setBytes(String key, byte[] value);
	
	public byte[] getBytes(String key);
	
	public Map<String, byte[]> getBytes(String[] keys);
		
	public String setObject(String key, Object value);
	
	public Object getObject(String key);
	
	public Map<String, Object> getObject(String[] keys);
	
	public String set(String key, String value);
	
	/**
	 * Set the string value as value of the key. The string can't be longer than
	 * 1073741824 bytes (1 GB).
	 * 
	 * @param key
	 * @param value
	 * @param nxxx  NX|XX, NX -- Only set the key if it does not already exist. XX
	 *              -- Only set the key if it already exist.
	 * @param expx  EX|PX, expire time units: EX = seconds; PX = milliseconds
	 * @param time  expire time in the units of <code>expx</code>
	 * @return Status code reply
	 */
	public String set(String key, String value, String nxxx, String expx, int time);
	
	public String get(String key);
	
	/**
	   * Increment the number stored at key by one. If the key does not exist or contains a value of a
	   * wrong type, set the key to the value of "0" before to perform the increment operation.
	   * <p>
	   * INCR commands are limited to 64 bit signed integers.
	   * <p>
	   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
	   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
	   * and then converted back as a string.
	   * <p>
	   * Time complexity: O(1)
	   * @see #incrBy(String, long)
	   * @see #decr(String)
	   * @see #decrBy(String, long)
	   * @param key
	   * @return Integer reply, this commands will reply with the new value of key after the increment.
	   */
	public Long incr(String key);
	
	/**
	   * Decrement the number stored at key by one. If the key does not exist or contains a value of a
	   * wrong type, set the key to the value of "0" before to perform the decrement operation.
	   * <p>
	   * INCR commands are limited to 64 bit signed integers.
	   * <p>
	   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
	   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
	   * and then converted back as a string.
	   * <p>
	   * Time complexity: O(1)
	   * @see #incr(String)
	   * @see #incrBy(String, long)
	   * @see #decrBy(String, long)
	   * @param key
	   * @return Integer reply, this commands will reply with the new value of key after the increment.
	   */
	public Long decr(String key);
	
	/**
	   * INCRBY work just like {@link #incr(String) INCR} but instead to increment by 1 the increment is
	   * integer.
	   * <p>
	   * INCR commands are limited to 64 bit signed integers.
	   * <p>
	   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
	   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
	   * and then converted back as a string.
	   * <p>
	   * Time complexity: O(1)
	   * @see #incr(String)
	   * @see #decr(String)
	   * @see #decrBy(String, long)
	   * @param key
	   * @param integer
	   * @return Integer reply, this commands will reply with the new value of key after the increment.
	   */
	public Long incrBy(String key, long integer);
	
	/**
	   * IDECRBY work just like {@link #decr(String) INCR} but instead to decrement by 1 the decrement
	   * is integer.
	   * <p>
	   * INCR commands are limited to 64 bit signed integers.
	   * <p>
	   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
	   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
	   * and then converted back as a string.
	   * <p>
	   * Time complexity: O(1)
	   * @see #incr(String)
	   * @see #decr(String)
	   * @see #incrBy(String, long)
	   * @param key
	   * @param integer
	   * @return Integer reply, this commands will reply with the new value of key after the increment.
	   */
	public Long decrBy(String key, long integer);
	
	public Map<String, String> get(String[] keys);
	
	public Long del(String key);
	
	/**
	 * SETNX works exactly like SET with the only difference that if thekey already exists no operation is performed. SETNX actually means "SET if Not eXists". 
	 * Time complexity: O(1)
	 * @param key
	 * @param value
	 * @return Integer reply, specifically: 1 if the key was set 0 if the key was not set
	 */
	public Long setnxBytes(String key, byte[] value);
	
	/**
	 * SETNX works exactly like SET with the only difference that if thekey already exists no operation is performed. SETNX actually means "SET if Not eXists". 
	 * Time complexity: O(1)
	 * @param key
	 * @param value
	 * @return Integer reply, specifically: 1 if the key was set 0 if the key was not set
	 */
	public Long setnx(String key, String value);
	
	/**
	 * SETNX works exactly like SET with the only difference that if thekey already exists no operation is performed. SETNX actually means "SET if Not eXists". 
	 * Time complexity: O(1)
	 * @param key
	 * @param value
	 * @return Integer reply, specifically: 1 if the key was set 0 if the key was not set
	 */
	public Long setnxObject(String key, Object value);
	
	/**
	   * If the key already exists and is a string, this command appends the provided value at the end
	   * of the string. If the key does not exist it is created and set as an empty string, so APPEND
	   * will be very similar to SET in this special case.
	   * <p>
	   * Time complexity: O(1). The amortized time complexity is O(1) assuming the appended value is
	   * small and the already present value is of any size, since the dynamic string library used by
	   * Redis will double the free space available on every reallocation.
	   * @param key
	   * @param value
	   * @return Integer reply, specifically the total length of the string after the append operation.
	   */
	public Long append(String key, String value);
	
	public String flushDB();
	
	public Long dbSize();
	
	public String info(String section);
	
	public String info();
	
	public String hget(String key,String hashKey);
	
	public byte[] hgetBytes(String key,String hashKey);
	
	public Object hgetObject(String key,String hashKey);
	
	public Boolean hExists(String key,String hashKey);
	
	public Boolean exists(String key);
	
	public Long hset(String key,String hashKey,String hashVal);
	
	public Long hsetBytes(String key,String hashKey,Object hashVal);
	
	public String hmset(String key,Map<String,String> map);
	
	public String hmsetObject(String key,Map<String,Object> map);
	
	public Map<String,Object>hgetAllObject(String key);
	
	public Set<String> hkeys(String key);
	
	public String rename(String oldkey,String newkey);
	
	public long zadd(String key,String value,double score);
	
	public long zrem(String key,String[] value);
	
	public long expire(String key,int seconds);
	
	public Set<String> getKeys(String pattern);
	
	public Long zcount(String key, String max, String min);
	
	public LinkedHashSet<String> zrevrangebyscore(String key, String max, String min, int offset, int count);

	public  <T> T  hgetGObject(String key,String hashKey,Class<T> clazz);
	
	public long sadd(String key,String[] members);
	
	public boolean sismember(String key,String value);
	
	public long hdel(String key,String field);
	
	public long hdel(byte[] key,byte[] field);
	
	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params);
	
	public Set<String> zrange(String key,long start,long end);

}
