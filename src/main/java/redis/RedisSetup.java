package redis;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class RedisSetup {
    public static FlinkJedisPoolConfig redisSetup(){
        FlinkJedisPoolConfig setup = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        return setup;
    }
}
