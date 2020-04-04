package cn.mybop.redisclient.balance;

import java.util.List;
import java.util.Random;

public class RandomLoadBalancer implements LoadBalancer {
	
	private Random rand = new Random(System.nanoTime());

	@Override
	public int selectServer(List<String> servers) {
		int index = 0;
		if (servers != null) {
			int size = servers.size();
			if (size > 1) {
				index = rand.nextInt(size);
			}
		}
		return index;
	}

}
