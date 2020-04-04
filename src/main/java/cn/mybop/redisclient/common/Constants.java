package cn.mybop.redisclient.common;

public interface Constants {
	
	public static interface Serialization {
		
		public final static String HESSIAN = "hessian";
		
		public final static String JAVA = "java";
		
		public final static String JSON = "json";
		
	}
	
	public static interface Charset {
		
		public final static String UTF8 = "utf-8";
		
	}
	
	public static interface Loadbalancer {
		
		public final static String RANDOM = "random";
		
		public final static String ROUNDROBIN = "roundrobin";
		
	}
	
	public final static String DEFAULT_LOADBALANCER = Loadbalancer.RANDOM;
	
	public final static String VERSION_KEY = "version.key";
	
	public final static String DEFAULT_VERSION_KEY = "version.No";
	
	public final static String CHECK_TASK = "check.task";
	
	public final static String VALIDATE_CLASS = "validate.class";
	
	public final static String VALIDATE_KEY = "validate.key";
	
	public final static String VALIDATE_VALUE = "validate.value";
	
	public final static String DEFAULT_CHARSET = Charset.UTF8;
	
	public final static String CLIENT_NAME = "client.name";
	
	public final static String CLIENT_TYPE = "client.type";
	
	public final static String CLIENT_TYPE_DEFAULT = "default";
	
	public final static String CLIENT_TYPE_SENTINEL = "sentinel";
	
	public final static String CLIENT_TYPE_READONLY = "readonly";
	
	public final static String CLIENT_TYPE_WRITEALLREADANY = "writeallreadany";
	
	public final static String CLIENT_TYPE_SHARED = "shared";
	
	public final static String SERIALIZABLE = "serializable";
	
	public final static String DEFAULT_SERIALIZABLE = Serialization.JAVA;
	
	public final static String SERVER_RETRY_COUNT = "server.retry.count";
	
	public final static String SERVER_RETRY_SLEEPTIME = "server.retry.sleeptime";
	
	public final static String SHARED_ALGORITHM = "shared.algorithm";
	
	public final static String DEFAULT_SHARED_ALGORITHM = "hash";
	
	public final static String SHARED_NODE = "shared.node";
	
	public final static int DEFAULT_SHARED_NODE = 160;
	
	public final static String SHARED_CLIENT_LIST = "shared.client.list";
	
	public final static String SERVER_LIST = "server.list";
	
	public final static String SENTINEL_LIST = "sentinel.list";
	
	public final static String SENTINEL_MASTER_NAME = "sentinel.master.name";
	
	public final static String SERVER_TIMEOUT = "server.timeout";
	
	public final static String SERVER_DATABASE = "server.database";
	
	public final static String SERVER_PASSWORD = "server.password";
	
	public final static String SERVER_LOADBALANCER = "server.loadBalancer";
	
	public final static String SERVER_NAMESPACE = "server.namespace";
	
	public final static String CHECK_SCHEDULE_INITIAL_DELAY = "check.schedule.initial.delay";
	
	public final static String CHECK_SCHEDULE_DELAY = "check.schedule.delay";
	
	public final static long DEFAULT_CHECK_SCHEDULE_INITIAL_DELAY = 2l;
	
	public final static long DEFAULT_CHECK_SCHEDULE_DELAY = 3l;

	public final static String POOL_MAX_ACTIVE = "pool.maxActive";
	
	public final static String POOL_MAX_IDLE = "pool.maxIdle";
	
	public final static String POOL_MIN_IDLE = "pool.minIdle";
	
	public final static String POOL_TEST_ON_BORROW = "pool.testOnBorrow";
	
	public final static String POOL_TEST_ON_RETURN = "pool.testOnReturn";
	
	public final static String POOL_TEST_WHILE_IDLE = "pool.testWhileIdle";
	
	public final static String POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS = "pool.timeBetweenEvictionRunsMillis";
	
	public final static String POOL_TIME_EVICTABLE_IDLE_TIME_MILLIS = "pool.minEvictableIdleTimeMillis";
	
	public final static String POOL_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = "pool.softMinEvictableIdleTimeMillis";
	
	public final static String POOL_NUM_TESTS_PER_EVICTION_RUN = "pool.numTestsPerEvictionRun";
	
	public final static String COMPRESS_THRESHOLD = "compress.threshold";
	
	public final static String MAX_BYTE_SIZE = "max.byte.size";
	
	public final static byte COMPRESS_FLAG = 1;
	
	public final static String WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR = "writeall.throwExceptionWhenWriteError";
	
	public final static boolean DEFAULT_WRITEALL_THROW_EXCEPTION_WHEN_WRITE_ERROR = false;
	
	public final static String REPLY_CODE_OK = "OK";
	
}
