package redis;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisMapperImpl implements RedisMapper<Tuple3<String,String,String>> {

    @Override
    public RedisCommandDescription getCommandDescription(){
        return new RedisCommandDescription(RedisCommand.SET);
    }

    @Override
    public String getKeyFromData(Tuple3<String,String,String> data){
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple3<String,String,String> data){
        return data.f1;
    }
}