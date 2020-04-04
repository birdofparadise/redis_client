package cn.mybop.redisclient.lifecycle;

public class LifecycleException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2855015536761088192L;

	public LifecycleException(String message) {
		super(message);
	}

	public LifecycleException(Throwable cause) {
		super(cause);
	}

	public LifecycleException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
