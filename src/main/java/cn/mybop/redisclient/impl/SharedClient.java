package cn.mybop.redisclient.impl;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import cn.mybop.redisclient.IRedisClient;
import cn.mybop.redisclient.RedisClientFactory;
import cn.mybop.redisclient.RedisException;
import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import cn.mybop.redisclient.lifecycle.LifecycleBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.util.Hashing;
import redis.clients.util.MurmurHash;

public class SharedClient extends LifecycleBase implements IRedisClient {
	
	private TreeMap<Long, IRedisClient> nodes;
	
	private Map<String, IRedisClient> clients;
	
	private Hashing algo;
	
	private int sharedNode;
	
	private Properties props;
	
	private String name;
	
	public SharedClient(Properties props) {
		this.props = props;
	}
	
	@Override
	protected void startInternal() {
		// client name
		name = props.getProperty(Constants.CLIENT_NAME);
		if (Utils.isBlank(name)) {
			throw new RedisException(Constants.CLIENT_NAME + "参数未定义");
		}
				
		String strSharedClients = props.getProperty(Constants.SHARED_CLIENT_LIST);
		if (Utils.isBlank(strSharedClients)) {
			throw new RedisException(Constants.SHARED_CLIENT_LIST + "参数为空");
		}
		
		String shareAlgorithm = props.getProperty(Constants.SHARED_ALGORITHM, Constants.DEFAULT_SHARED_ALGORITHM);
		if (Constants.DEFAULT_SHARED_ALGORITHM.equalsIgnoreCase(shareAlgorithm)) {
			algo = new MurmurHash();
		} else {
			throw new RedisException("暂不支持类型算法");
		}
		
		String strShareNode = props.getProperty(Constants.SHARED_NODE);
		if (Utils.isNotBlank(strShareNode)) {
			sharedNode = Integer.parseInt(strShareNode);
		} else {
			sharedNode = Constants.DEFAULT_SHARED_NODE;
		}
		
		nodes = new TreeMap<Long, IRedisClient>();
		clients = new HashMap<String, IRedisClient>();
		String[] sharedClients = strSharedClients.split(",");
		for (int i = 0; i < sharedClients.length; i++) {
			Properties clientProps = new Properties();
			clientProps.putAll(props);
			clientProps.putAll(RedisClientFactory.getProperties(sharedClients[i]));
			//不允许shared redis client再嵌套shared redis client
			String clientType = clientProps.getProperty(Constants.CLIENT_TYPE);
			if (Utils.isNotBlank(clientType) && Constants.CLIENT_TYPE_SHARED.equalsIgnoreCase(clientType)) {
				throw new RedisException("不允许shared redis client再嵌套shared redis client");
			}
			IRedisClient redisClient = RedisClientFactory.getClient(clientProps);
			for (int n = 0; n < sharedNode; n++) {
				nodes.put(algo.hash("SHARD-" + i + "-NODE-" + n), redisClient);
			}
			clients.put(redisClient.getName(), redisClient);
		}
	}
	
	@Override
	protected void stopInternal() {
		nodes.clear();
		for (Entry<String, IRedisClient> entry : clients.entrySet()) {
			if (entry.getValue() != null) {
				entry.getValue().stop();
			}
		}
		clients.clear();
		
		nodes = null;
		clients = null;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getNamespace() {
		return null;
	}

	@Override
	public RedisManager getManager() {
		throw new RedisException("shared redis client do not have redis manager");
	}
	
	private IRedisClient getShard(byte[] key) {
		SortedMap<Long, IRedisClient> tail = nodes.tailMap(algo.hash(key));
		if (tail.isEmpty()) {
			return nodes.get(nodes.firstKey());
		}
		return tail.get(tail.firstKey());
	}
	
	private IRedisClient getShard(String key) {
		try {
			return getShard(key.getBytes(Constants.DEFAULT_CHARSET));
		} catch (UnsupportedEncodingException e) {
			throw new RedisException(e);
		}
	}

	@Override
	public String setBytes(String key, byte[] value) {
		return getShard(key).setBytes(key, value);
	}
	
	public Long incr(String key) {
		return getShard(key).incr(key);
	}
	
	public Long incrBy(String key, long integer) {
		return getShard(key).incrBy(key, integer);
	}
	
	public Long decr(String key) {
		return getShard(key).decr(key);
	}
	
	public Long decrBy(String key, long integer) {
		return getShard(key).decrBy(key, integer);
	}

	@Override
	public byte[] getBytes(String key) {
		return getShard(key).getBytes(key);
	}

	@Override
	public Map<String, byte[]> getBytes(String[] keys) {
		Map<String, List<String>> keyMap = new HashMap<String, List<String>>();
		Map<String, IRedisClient> clientMap = new HashMap<String, IRedisClient>();
		for (int i = 0; i < keys.length; i++) {
			IRedisClient redisClient = getShard(keys[i]);
			if (keyMap.containsKey(redisClient.getName())) {
				keyMap.get(redisClient.getName()).add(keys[i]);
			} else {
				List<String> list = new LinkedList<String>();
				list.add(keys[i]);
				keyMap.put(redisClient.getName(), list);
				clientMap.put(redisClient.getName(), redisClient);
			}
		}
		Map<String, byte[]> rtn =  new HashMap<String, byte[]>();
		for (Entry<String, List<String>> entry : keyMap.entrySet()) {
			String[] tmpkeys = entry.getValue().toArray(new String[0]);
			rtn.putAll(clientMap.get(entry.getKey()).getBytes(tmpkeys));
		}
		return rtn;
	}

	@Override
	public String setObject(String key, Object value) {
		return getShard(key).setObject(key, value);
	}

	@Override
	public Object getObject(String key) {
		return getShard(key).getObject(key);
	}

	@Override
	public Map<String, Object> getObject(String[] keys) {
		Map<String, List<String>> keyMap = new HashMap<String, List<String>>();
		Map<String, IRedisClient> clientMap = new HashMap<String, IRedisClient>();
		for (int i = 0; i < keys.length; i++) {
			IRedisClient redisClient = getShard(keys[i]);
			if (keyMap.containsKey(redisClient.getName())) {
				keyMap.get(redisClient.getName()).add(keys[i]);
			} else {
				List<String> list = new LinkedList<String>();
				list.add(keys[i]);
				keyMap.put(redisClient.getName(), list);
				clientMap.put(redisClient.getName(), redisClient);
			}
		}
		Map<String, Object> rtn =  new HashMap<String, Object>();
		for (Entry<String, List<String>> entry : keyMap.entrySet()) {
			String[] tmpkeys = entry.getValue().toArray(new String[0]);
			rtn.putAll(clientMap.get(entry.getKey()).getObject(tmpkeys));
		}
		return rtn;
	}

	@Override
	public String set(String key, String value) {
		return getShard(key).set(key, value);
	}
	
	public String set(String key, String value, String nxxx, String expx, int time) {
		return getShard(key).set(key, value, nxxx, expx, time);
	}

	@Override
	public String get(String key) {
		return getShard(key).get(key);
	}

	@Override
	public Map<String, String> get(String[] keys) {
		Map<String, List<String>> keyMap = new HashMap<String, List<String>>();
		Map<String, IRedisClient> clientMap = new HashMap<String, IRedisClient>();
		for (int i = 0; i < keys.length; i++) {
			IRedisClient redisClient = getShard(keys[i]);
			if (keyMap.containsKey(redisClient.getName())) {
				keyMap.get(redisClient.getName()).add(keys[i]);
			} else {
				List<String> list = new LinkedList<String>();
				list.add(keys[i]);
				keyMap.put(redisClient.getName(), list);
				clientMap.put(redisClient.getName(), redisClient);
			}
		}
		Map<String, String> rtn =  new HashMap<String, String>();
		for (Entry<String, List<String>> entry : keyMap.entrySet()) {
			String[] tmpkeys = entry.getValue().toArray(new String[0]);
			rtn.putAll(clientMap.get(entry.getKey()).get(tmpkeys));
		}
		return rtn;
	}

	@Override
	public Long del(String key) {
		return getShard(key).del(key);
	}

	@Override
	public String flushDB() {
		StringBuilder sb = new StringBuilder();
		for (Entry<String, IRedisClient> entry : clients.entrySet()) {
			sb.append(entry.getKey()).append("=").append(entry.getValue().flushDB()).append(System.getProperty("line.separator"));
		}
		return sb.toString();
	}

	@Override
	public Long dbSize() {
		long totalSize = 0l;
		for (Entry<String, IRedisClient> entry : clients.entrySet()) {
			totalSize += entry.getValue().dbSize();
		}
		return Long.valueOf(totalSize);
	}

	@Override
	public String info(String section) {
		StringBuilder sb = new StringBuilder();
		for (Entry<String, IRedisClient> entry : clients.entrySet()) {
			sb.append(entry.getKey()).append("=").append(entry.getValue().info(section)).append(System.getProperty("line.separator"));
		}
		return sb.toString();
	}

	@Override
	public String info() {
		StringBuilder sb = new StringBuilder();
		for (Entry<String, IRedisClient> entry : clients.entrySet()) {
			sb.append(entry.getKey()).append("=").append(entry.getValue().info()).append(System.getProperty("line.separator"));
		}
		return sb.toString();
	}

	@Override
	public String hget(String key, String hashKey) {
		return getShard(key).hget(key, hashKey);
	}

	@Override
	public byte[] hgetBytes(String key, String hashKey) {
		return getShard(key).hgetBytes(key, hashKey);
	}

	@Override
	public Object hgetObject(String key, String hashKey) {
		return getShard(key).hget(key, hashKey);
	}

	@Override
	public Boolean hExists(String key, String hashKey) {
		return getShard(key).hExists(key, hashKey);
	}
	
	@Override
	public Boolean exists(String key) {
		return getShard(key).exists(key);
	}

	@Override
	public Long hset(String key, String hashKey, String hashVal) {
		return getShard(key).hset(key, hashKey, hashVal);
	}

	@Override
	public String hmset(String key, Map<String, String> map) {
		return getShard(key).hmset(key, map);
	}
	@Override
	public String hmsetObject(String key,Map<String,Object> map) {
		return getShard(key).hmsetObject(key, map);
	}
	@Override
	public Map<String,Object>hgetAllObject(String key){
		return getShard(key).hgetAllObject(key);
	}
	
	@Override
	public Set<String> hkeys(String key) {
		return getShard(key).hkeys(key);
	}

	@Override
	public String rename(String oldkey, String newkey) {
		throw new RedisException("shared redis client do not support rename operation!");
	}

	@Override
	public long zadd(String key, String value, double score) {
		return getShard(key).zadd(key, value, score);
	}

	@Override
	public LinkedHashSet<String> zrevrangebyscore(String key, String max, String min, int offset, int count) {
		return getShard(key).zrevrangebyscore(key, max, min, offset, count);
	}

	@Override
	public Long hsetBytes(String key, String hashKey, Object hashVal) {
		return getShard(key).hsetBytes(key, hashKey, hashVal);
	}

	@Override
	public long zrem(String key, String[] value) {
		return getShard(key).zrem(key, value);
	}

	@Override
	public long expire(String key, int seconds) {
		return getShard(key).expire(key, seconds);
	}
	
	public Set<String> getKeys(String pattern){
		throw new RedisException("shared redis client do not support getKeys operation!");
	}

	@Override
	public Long zcount(String key, String max, String min) {
		return getShard(key).zcount(key, max, min);
	}
	
	public  <T> T  hgetGObject(String key,String hashKey,Class<T> clazz){
		return getShard(key).hgetGObject(key, hashKey, clazz);
	}

	@Override
	public long sadd(String key, String[] members) {
		return getShard(key).sadd(key, members);
	}

	@Override
	public boolean sismember(String key, String value) {
		return getShard(key).sismember(key, value);
	}
	
	public long hdel(String key,String field){
		return getShard(key).hdel(key, field);
	}
	
	public long hdel(byte[] key,byte[] field){
		return getShard(key).hdel(key, field);
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
		 return getShard(key).zscan(key, cursor, params);
	}

	@Override
	public Set<String> zrange(String key, long start, long end) {
		return getShard(key).zrange(key, start, end);
	}

	@Override
	public Long setnxBytes(String key, byte[] value) {
		return getShard(key).setnxBytes(key, value);
	}

	@Override
	public Long setnx(String key, String value) {
		return getShard(key).setnx(key, value);
	}

	@Override
	public Long setnxObject(String key, Object value) {
		return getShard(key).setnxObject(key, value);
	}

	@Override
	public Long append(String key, String value) {
		return getShard(key).append(key, value);
	}

	@Override
	public RedisManager initManager() {
		return null;
	}
	
}
