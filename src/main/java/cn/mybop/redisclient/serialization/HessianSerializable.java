package cn.mybop.redisclient.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import com.caucho.hessian.io.SerializerFactory;

public class HessianSerializable implements ISerializable {
	
	private final SerializerFactory sf = new SerializerFactory();

	@Override
	public byte[] object2bytes(Object obj) {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		HessianOutput out = new HessianOutput(b);
	    out.setSerializerFactory(sf);
	    try {
		    out.writeObject(obj);
		    out.flush();
	    } catch (Exception e) {
	    	throw new SerializableException(e);
	    }
	    return b.toByteArray();
	}

	@Override
	public Object bytes2object(byte[] bytes) {
		ByteArrayInputStream b = new ByteArrayInputStream(bytes);
		HessianInput in = new HessianInput(b);
		in.setSerializerFactory(sf);
		Object rtn = null;
		try {
			rtn = in.readObject();
			b.close();
		} catch (Exception e) {
	    	throw new SerializableException(e);
	    }		
		return rtn;
	}

	@Override
	public <T> T bytes2object(byte[] bytes, Class<T> clazz) {
		return (T) bytes2object(bytes);
	}

}
