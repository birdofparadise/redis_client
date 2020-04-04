package cn.mybop.redisclient;

public class RedisException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1537635703478044248L;

	public RedisException(String message) {
		super(message);
	}

	public RedisException(Throwable cause) {
		super(cause);
	}

	public RedisException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
