package cn.mybop.redisclient.balance;

import java.util.List;

public interface LoadBalancer {
	
	public int selectServer(List<String> servers);

}
