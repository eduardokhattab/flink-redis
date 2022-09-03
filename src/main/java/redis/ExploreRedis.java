package redis;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import postgresql.Challenge;

import static redis.RedisSetup.redisSetup;

public class ExploreRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hashName = "challenge";
        //hashName,score,userId
        Tuple3<String, String, String> tuple = Tuple3.of("eduardo", "1500", hashName);
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
