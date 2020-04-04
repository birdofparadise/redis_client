package cn.mybop.redisclient.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

public class Utils {
		
	public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }
	
	public static boolean isNotBlank(String str) {
		return !Utils.isBlank(str);
	}

	public static String mergeKey(String namespace, String key) {
		if (Utils.isNotBlank(namespace)) {
			return namespace + ":" + key;
		}
		return key;
	}
	
	public static byte[] gzip(byte[] bytes) throws IOException {
		ByteArrayOutputStream bos = null;
		GZIPOutputStream gos = null;
		try {
			bos = new ByteArrayOutputStream(bytes.length);
			gos = new GZIPOutputStream(bos);
			gos.write(bytes, 0, bytes.length);
			gos.finish();
			return bos.toByteArray();
		} finally {
			if (gos != null) {
				try {
					gos.close();
				} catch (IOException e) {
					
				}
			}
			if (bos != null) {
				try {
					bos.close();
				} catch (IOException e) {
					
				}
			}			
		}
	}
	
	public static byte[] unGzip(byte[] bytes, int len) throws IOException {
		byte[] bs = new byte[len];
		GZIPInputStream gzi = null;
		ByteArrayOutputStream bos = null;
		try {
			gzi = new GZIPInputStream(new ByteArrayInputStream(bytes));
			bos = new ByteArrayOutputStream(bs.length);
			int count = 0;
			byte[] tmp = new byte[2048];
			while ((count = gzi.read(tmp)) != -1) {
				bos.write(tmp, 0, count);
			}	
			return bos.toByteArray();
		} finally {
			if (gzi != null) {
				try {
					gzi.close();
				} catch (IOException e) {
					
				}
			}
			if (bos != null) {
				try {
					bos.close();
				} catch (IOException e) {
					
				}
			}			
		}
	}
	
	public static byte[] getOrigBytes(byte[] bytes, int compressThreshold) throws IOException {
		if (bytes == null || bytes.length == 0) {
			return bytes;
		}
		if (compressThreshold > 0) {
			//压缩标志
			byte flag = bytes[0];
			if (flag == Constants.COMPRESS_FLAG) {
				//长度
				byte[] lenBytes = new byte[4];
				System.arraycopy(bytes, 1, lenBytes, 0, 4);
				int len = Utils.bytesToInt(lenBytes);
				//对象
				byte[] gzipBytes = new byte[bytes.length - 5];
 				System.arraycopy(bytes, 5, gzipBytes, 0, gzipBytes.length);
				return Utils.unGzip(gzipBytes, len);
			} else {
				byte[] origBytes = new byte[bytes.length - 1];
				System.arraycopy(bytes, 1, origBytes, 0, origBytes.length);
				return origBytes;
			}
		} else {
			return bytes;
		}
	}
	
	public static byte[] getCompressBytes(byte[] bytes, int compressThreshold) throws IOException {
		if (bytes == null || bytes.length == 0) {
			return bytes;
		}
		if (compressThreshold > 0) {
			byte[] lenBytes = Utils.intToBytes(bytes.length);
			byte[] compressBytes = null;
			//压缩标志
			if (bytes.length > compressThreshold) {
				byte[] gzipBytes = Utils.gzip(bytes);
				compressBytes = new byte[gzipBytes.length + 5];
				//压缩标志
				compressBytes[0] = Constants.COMPRESS_FLAG;
				//长度
				System.arraycopy(lenBytes, 0, compressBytes, 1, lenBytes.length);
				//对象
				System.arraycopy(gzipBytes, 0, compressBytes, 5, gzipBytes.length);
			} else {
				compressBytes = new byte[bytes.length + 1];
				//对象
				System.arraycopy(bytes, 0, compressBytes, 1, bytes.length);
			}			
			return compressBytes;
		} else {
			return bytes;
		}
	}
	
	public static int bytesToInt(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}
	
	public static byte[] intToBytes(int i) {
		return ByteBuffer.allocate(4).putInt(i).array();
	}
	
	/**
	 * 返回jedis连接的host和port
	 * @param jedis
	 * @return ip:port
	 */
	public static String getHostAndPort(Jedis jedis) {
		return jedis.getClient().getHost() + ":" + jedis.getClient().getPort();
	}
	
	public static JedisPoolConfig initPoolConfig(Properties props) {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		String maxActive = props.getProperty(Constants.POOL_MAX_ACTIVE);
		if (Utils.isNotBlank(maxActive)) {
			poolConfig.setMaxTotal(Integer.parseInt(maxActive));
		}
		String maxIdle = props.getProperty(Constants.POOL_MAX_IDLE);
		if (Utils.isNotBlank(maxIdle)) {
			poolConfig.setMaxIdle(Integer.parseInt(maxIdle));
		}
		String minIdle = props.getProperty(Constants.POOL_MIN_IDLE);
		if (Utils.isNotBlank(minIdle)) {
			poolConfig.setMinIdle(Integer.parseInt(minIdle));
		}
		String testOnBorrow = props.getProperty(Constants.POOL_TEST_ON_BORROW);
		if (Utils.isNotBlank(testOnBorrow)) {
			poolConfig.setTestOnBorrow(Boolean.parseBoolean(testOnBorrow));
		}
		String testOnReturn = props.getProperty(Constants.POOL_TEST_ON_RETURN);
		if (Utils.isNotBlank(testOnReturn)) {
			poolConfig.setTestOnReturn(Boolean.parseBoolean(testOnReturn));
		}
		String testWhileIdle = props.getProperty(Constants.POOL_TEST_WHILE_IDLE);
		if (Utils.isNotBlank(testWhileIdle)) {
			poolConfig.setTestWhileIdle(Boolean.parseBoolean(testWhileIdle));
		}
		String timeBetweenEvictionRunsMillis = props.getProperty(Constants.POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
		if (Utils.isNotBlank(timeBetweenEvictionRunsMillis)) {
			poolConfig.setTimeBetweenEvictionRunsMillis(Long.parseLong(timeBetweenEvictionRunsMillis));
		}
		String minEvictableIdleTimeMillis = props.getProperty(Constants.POOL_TIME_EVICTABLE_IDLE_TIME_MILLIS);
		if (Utils.isNotBlank(minEvictableIdleTimeMillis)) {
			poolConfig.setMinEvictableIdleTimeMillis(Long.parseLong(timeBetweenEvictionRunsMillis));
		}
		String softMinEvictableIdleTimeMillis = props.getProperty(Constants.POOL_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
		if (Utils.isNotBlank(softMinEvictableIdleTimeMillis)) {
			poolConfig.setSoftMinEvictableIdleTimeMillis(Long.parseLong(softMinEvictableIdleTimeMillis));
		}
		String numTestsPerEvictionRun = props.getProperty(Constants.POOL_NUM_TESTS_PER_EVICTION_RUN);
		if (Utils.isNotBlank(numTestsPerEvictionRun)) {
			poolConfig.setNumTestsPerEvictionRun(Integer.parseInt(softMinEvictableIdleTimeMillis));
		}
		return poolConfig;
	}
}
