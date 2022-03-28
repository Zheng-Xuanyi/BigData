package com.zxy.sink;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author zxy
 * @create 2022-03-01 8:42
 */
public class RedisSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDs = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        SingleOutputStreamOperator<SensorReading> sensorDs = stringDs.map( (s) -> {
                    String[] split = s.split(",");
                    return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
                }
        );

        //redis连接池配置
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop101")
                .setPort(6379)
                .build();

        sensorDs.addSink(new RedisSink<SensorReading>(flinkJedisPoolConfig, new MyRedisMapper()));

        env.execute();
    }

    //定义一个redis的mapper类，用于定义保存到redis时调用的命令
    public static class MyRedisMapper implements RedisMapper<SensorReading> {

        // 保存到redis的命令，存成哈希表
        @Override
        public RedisCommandDescription getCommandDescription() {
            //RedisCommand枚举类型，及 表名
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return String.valueOf(sensorReading.getTemperature());
        }
    }
}
