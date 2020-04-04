package cn.mybop.redisclient.check;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.mybop.redisclient.RedisManager;
import cn.mybop.redisclient.common.Constants;
import cn.mybop.redisclient.common.Utils;
import cn.mybop.redisclient.serialization.ISerializable;
import cn.mybop.redisclient.validate.IValidate;
import cn.mybop.redisclient.validate.NoNeedValidateImpl;
import redis.clients.jedis.Jedis;

public abstract class AbstractCheckTask implements ICheckTask {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCheckTask.class);
	
	private RedisManager manager;
	
	private ISerializable serializable;
	
	private Properties props;
	
	private IValidate iValidate;
	
	public AbstractCheckTask(RedisManager manager, ISerializable serializable, Properties props) {
		this.manager = manager;
		this.props = props;
		if (Utils.isNotBlank(props.getProperty(Constants.VALIDATE_CLASS))) {
			try {
				this.iValidate = (IValidate) Class.forName(props.getProperty(Constants.VALIDATE_CLASS)).newInstance();
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error("加载redis validate类" + props.getProperty(Constants.VALIDATE_CLASS) + "]失败 使用默认validate类", e);
				}
				this.iValidate = new NoNeedValidateImpl();
			}
		} else {
			this.iValidate = new NoNeedValidateImpl();
		}
		this.serializable = serializable;
	}
	
	public void run() {
		exec();
	}
	
	public boolean checkPing(Jedis jedis) {
		if (!"PONG".equals(jedis.ping())) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis[" + jedis.getClient().getHost() + ":" + jedis.getClient().getPort() + "]ping失败");
			}
			return false;
		}
		return true;
	}
	
	public boolean checkValidate(Jedis jedis) {
		boolean rtn = iValidate.validate(jedis, serializable, props);
		if (!rtn) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("redis[" + jedis.getClient().getHost() + ":" + jedis.getClient().getPort() + "]validate失败");
			}
		}
		return rtn;
	}

	public RedisManager getManager() {
		return manager;
	}

	public ISerializable getSerializable() {
		return serializable;
	}

	public Properties getProps() {
		return props;
	}

}
