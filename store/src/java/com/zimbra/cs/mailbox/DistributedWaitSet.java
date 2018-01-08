package com.zimbra.cs.mailbox;

import java.util.List;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;

public class DistributedWaitSet {
	private static RedissonClient redisson = RedissonClientHolder.getInstance().getRedissonClient();

	public DistributedWaitSet() {
	}

	public static <T> long publish(String topic, T message) {
		return getTopic(topic, message).publish(message);
	}

	public static <T> RTopic<T> getTopic(String topic, T typeReference) {
		RTopic<T> rTopic = redisson.getTopic(topic);
		return rTopic;
	}

	public static <T> List<String> getChannelNames(String topic, T message) {
		return getTopic(topic, message).getChannelNames();
	}
}
