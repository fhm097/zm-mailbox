package com.zimbra.cs.mailbox;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public final class RedissonConn {
	private final Config config;
	private final RedissonClient redisson;
	private final static String HOST = "redis";
	private final static String PORT = "6379";
	public final static RedissonConn INSTANCE = new RedissonConn();

	private RedissonConn() {
		this.config = new Config();
		this.config.useSingleServer().setAddress("redis://" + HOST + ":" + PORT);
		this.redisson = Redisson.create(this.config);
	}

	public RedissonClient getRedissonClient() {
		return redisson;
	}
}
