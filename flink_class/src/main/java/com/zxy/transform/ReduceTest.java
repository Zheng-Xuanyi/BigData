package com.zxy.transform;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zxy
 * @create 2022-02-20 23:07
 */
public class ReduceTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> strDataStream = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map( (s) -> {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        );

//        DataStream<SensorReading> reduceStream = sensorDs.keyBy("id").reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                return new SensorReading(
//                        value1.getId(),
//                        value2.getTimestamp(),
//                        Math.min(value1.getTemperature(), value2.getTemperature()));
//            }
//        });

        DataStream<SensorReading> reduceStream = sensorDs.keyBy("id").reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(
                        value1.getId(),
                        value2.getTimestamp(),
                        Math.min(value1.getTemperature(), value2.getTemperature()));
            }
        });

        reduceStream.print();

        env.execute();
    }
}
