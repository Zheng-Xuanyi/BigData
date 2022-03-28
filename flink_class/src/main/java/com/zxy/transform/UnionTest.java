package com.zxy.transform;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author zxy
 * @create 2022-02-20 23:07
 */
public class UnionTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> strDataStream = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        SplitStream<SensorReading> splitDs = sensorDs.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> low = splitDs.select("low");
        DataStream<SensorReading> high = splitDs.select("high");

        DataStream<SensorReading> union = high.union(low);

        union.print("union");
        low.print("low");
        high.print("high");

        env.execute();
    }
}
