package com.zxy.window;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zxy
 * @create 2022-03-01 22:18
 */
public class CountWindowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        DataStreamSource<String> strDataStream = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        KeyedStream<SensorReading, Tuple> id = sensorDs.keyBy("id");


        SingleOutputStreamOperator<SensorReading> temperature = id
//                .windowAll()
                .countWindow(5)
                .minBy("temperature");


        temperature.print("window");

//        id.print("key_by");

        env.execute();
    }
}
