package redis;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class RedisSetup {
    public static FlinkJedisPoolConfig redisSetup(){
        return new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
    }
}
