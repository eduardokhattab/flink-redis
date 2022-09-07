package redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.util.CloseableIterator;
import postgresql.Challenge;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import static redis.RedisSetup.redisSetup;

public class ExploreRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hashName = "challenge";

        DataStream<Tuple2<String, String>> dataStreamRedis = env.addSource(new RichSourceFunction<>() {
            private JedisPool jedisPool = null;

            @Override
            public void run(SourceContext<Tuple2<String, String>> context) {
                jedisPool = new JedisPool(new JedisPoolConfig(), "127.0.0.1", 6379, Protocol.DEFAULT_TIMEOUT);
                try (Jedis jedis = jedisPool.getResource()) {
                   context.collect(Tuple2.of("key", jedis.get("key")));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void cancel() {
                try {
                    super.close();
                } catch (Exception ignored) {
                }
                if (jedisPool != null) {
                    jedisPool.close();
                    jedisPool = null;
                }
            }
        });

        CloseableIterator<Tuple2<String, String>> dataRedis = dataStreamRedis.executeAndCollect();
        String valueRedis = dataRedis.next().f1;
        //hashName,score,userId
        Tuple3<String, String, String> tuple = Tuple3.of("eduardo", valueRedis, hashName);
        DataStream<Tuple3<String, String, String>> dataStream = env.fromElements(tuple);

        FlinkJedisConfigBase setup = redisSetup();
        RedisSink<Tuple3<String, String, String>> redisSink = new RedisSink<>(setup, new RedisMapperImpl(hashName));

        dataStream.addSink(redisSink);

        String insertQuery = "insert into challenges (id, title, completed) values (?, ?, ?)";
        String jdbcUrl = "jdbc:postgresql://localhost:15432/challenge";
        String driverName = "org.postgresql.Driver";
        String username = "eduardo";
        String password = "teste123";

        env.fromElements(
            new Challenge(1L, "100K million steps", true)
        ).addSink(
                JdbcSink.sink(
                    insertQuery,
                    (statement, challenge) -> {
                        statement.setLong(1, challenge.id);
                        statement.setString(2, challenge.title);
                        statement.setBoolean(3, challenge.completed);
                    },
                    JdbcExecutionOptions.builder().build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName(driverName)
                        .withUsername(username)
                        .withPassword(password)
                        .build()
                )
        );

        env.execute("Redis + Postgresql Test");
    }
}
