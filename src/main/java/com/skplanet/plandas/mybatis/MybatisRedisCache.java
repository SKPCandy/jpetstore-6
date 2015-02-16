package com.skplanet.plandas.mybatis;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ibatis.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class MybatisRedisCache implements Cache {

	private static Logger logger = LoggerFactory.getLogger(MybatisRedisCache.class);
	private Jedis redisClient = createReids();
	/** The ReadWriteLock. */
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	private String id;

	public MybatisRedisCache(final String id) {
		if (id == null) {
			throw new IllegalArgumentException("Cache instances require an ID");
		}
		logger.debug("MybatisRedisCache :: MybatisRedisCache:id=" + id);
		this.id = id;
	}

	public String getId() {
		return this.id;
	}

	public int getSize() {

		return 1024 * 1024 * 30;
	}

	public void putObject(Object key, Object value) {
		logger.debug("MybatisRedisCache :: putObject:" + key + "=" + value);
		redisClient.set(SerializeUtil.serialize(key.toString()), SerializeUtil.serialize(value));
	}

	public Object getObject(Object key) {
		Object value = SerializeUtil.unserialize(redisClient.get(SerializeUtil.serialize(key.toString())));
		logger.debug("MybatisRedisCache :: getObject:" + key + "=" + value);
		return value;
	}

	public Object removeObject(Object key) {
		return redisClient.expire(SerializeUtil.serialize(key.toString()), 0);
	}

	public void clear() {
		//TODO if you needs , notice plandas team (plandas@skplanet.com)
		logger.info("not support flush method.");
	}

	public ReadWriteLock getReadWriteLock() {
		return readWriteLock;
	}

	protected static Jedis createReids() {
		JedisPool pool = new JedisPool(new JedisPoolConfig(), "devproxy.plandas.skplanet.com", 19010, Protocol.DEFAULT_TIMEOUT,
				"XXXXXX:XXXXXX");
		return pool.getResource();
	}
}