package cn.mybop.redisclient.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LifecycleBase implements Lifecycle {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(LifecycleBase.class);

	private volatile LifecycleState state = LifecycleState.NEW;
	
	protected abstract void startInternal();
	
	public final synchronized void start() {
		if (LifecycleState.STARTED.equals(state)) {
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info("lifecycleBase.alreadyStarted");
			}
			return;
		}
		
		if (LifecycleState.STOPPED.equals(state)) {
			throw new LifecycleException("lifecycleBase.alreadyStoped");
		}
		
		if (LifecycleState.FAILED.equals(state)) {
			stop();
		}
		
		try {
			startInternal();
		} catch (Throwable t) {
			setStateInternal(LifecycleState.FAILED);
			throw new LifecycleException("lifecycleBase.startFail", t);
		}
		
		setStateInternal(LifecycleState.STARTED);
	}
	
	protected abstract void stopInternal();

	public final synchronized void stop() {
		if (LifecycleState.STOPPED.equals(state)) {
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info("lifecycleBase.alreadyStopped");
			}
			return;
		}
		
		if (LifecycleState.NEW.equals(state)) {
			state = LifecycleState.STOPPED;
			return;
		}
		
		try {
			stopInternal();
		} catch (Throwable t) {
			setStateInternal(LifecycleState.FAILED);
			new LifecycleException("lifecycleBase.stopFail", t);
		}
		
		setStateInternal(LifecycleState.STOPPED);
	}
	
	private synchronized void setStateInternal(LifecycleState state) {
		this.state = state;
	}
	
	protected boolean isStarted() {
		return LifecycleState.STARTED.equals(state);
	}

}
