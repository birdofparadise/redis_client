package cn.mybop.redisclient.balance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinLoadBalancer implements LoadBalancer {

	private AtomicInteger count;
	
	public RoundRobinLoadBalancer() {
		count = new AtomicInteger(0);
	}
	
	@Override
	public int selectServer(List<String> servers) {
		int index = 0;
		if (servers != null) {
			int size = servers.size();
			if (size > 0) {
				index = count.getAndIncrement() % size;
			}
		}
		return index;
	}

}
