package com.zimbra.cs.mailbox;

import java.util.List;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;

import com.zimbra.cs.session.PendingModifications;
import com.zimbra.cs.session.Session;

public class DistributedWaitSet extends Session {

	private static RedissonClient redisson = RedissonClientHolder.getInstance().getRedissonClient();

	public DistributedWaitSet(String accountId, Type type) {
		super(accountId, type);
		// TODO Auto-generated constructor stub
	}

	public DistributedWaitSet(String authId, String targetId, Type type) {
		super(authId, targetId, type);
	}

	public <T> long publish(String topic, T message) {
		return getTopic(topic, message).publish(message);
	}

	public <T> RTopic<T> getTopic(String topic, T typeReference) {
		RTopic<T> rTopic = redisson.getTopic(topic);
		return rTopic;
	}

	public <T> List<String> getChannelNames(String topic, T message) {
		return getTopic(topic, message).getChannelNames();
	}

	@Override
	protected boolean isMailboxListener() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean isRegisteredInCache() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected long getSessionIdleLifetime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void notifyPendingChanges(PendingModifications pns, int changeId, Session source) {
		// TODO Auto-generated method stub
		this.publish("distributed", pns);
	}

	@Override
	protected void cleanup() {
		// TODO Auto-generated method stub

	}
}
