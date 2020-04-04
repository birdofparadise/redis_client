package cn.mybop.redisclient.serialization;

public class SerializableException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2888123450085975047L;

	public SerializableException(String message) {
		super(message);
	}

	public SerializableException(Throwable cause) {
		super(cause);
	}

	public SerializableException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
