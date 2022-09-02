package redis;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Optional;

public class RedisMapperImpl implements RedisMapper<Tuple3<String,String,String>> {

    String hashName;

    public RedisMapperImpl(String hashName) {
        this.hashName = hashName;
    }

    public RedisMapperImpl() {
        this(null);
    }

    @Override
    public RedisCommandDescription getCommandDescription(){
        return new RedisCommandDescription(RedisCommand.ZADD, this.hashName);
    }

    @Override
    public String getKeyFromData(Tuple3<String,String,String> data){
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple3<String,String,String> data){
        return data.f1;
    }

    @Override
    public Optional<String> getAdditionalKey(Tuple3<String, String, String> data) {
        return Optional.ofNullable(data.f2);
    }
}