package redis;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;

import static redis.RedisSetup.redisSetup;

public class ExploreRedis {
    public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		//user,score
		Tuple3<String, String, String> tuple = Tuple3.of("eduardo", "1000", "");
		DataStream<Tuple3<String,String,String>> dataStream = bsEnv.fromElements(tuple);

		FlinkJedisConfigBase setup = redisSetup();
		RedisSink<Tuple3<String, String, String>> redisSink = new RedisSink<>(setup, new RedisMapperImpl());

		dataStream.addSink(redisSink);
		bsEnv.execute("Redis Test");
	}
}
