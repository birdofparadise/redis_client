package cn.mybop.redisclient.serialization;

import cn.mybop.redisclient.common.Constants;

public class SerializableFactory {
	
	public static ISerializable getSerializable(String serializable) {
		if (Constants.Serialization.HESSIAN.equalsIgnoreCase(serializable)) {
			return new HessianSerializable();
		} else if (Constants.Serialization.JSON.equalsIgnoreCase(serializable)) {
			return new JsonSerializable();
		} else {
			return new JavaSerializable();
		} 
	}

}
