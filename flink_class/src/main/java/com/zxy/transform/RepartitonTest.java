package com.zxy.transform;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @author zxy
 * @create 2022-02-20 23:07
 */
public class RepartitonTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> strDataStream = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        sensorDs.shuffle().print("random");
        sensorDs.rebalance().print("Round-Robin");
        sensorDs.rescale().print("rescale");
        sensorDs.broadcast().print("broadcast");
        sensorDs.global().print("global");

        //custom自定义分区规则
//        DataStream<SensorReading> sensorReadingDataStream = sensorDs.partitionCustom(new Partitioner<String>() {
//            @Override
//            public int partition(String sensor_id, int i) {
//                return Integer.parseInt(sensor_id.split("_")[1]) % 4;
//            }
//        }, "id");

        DataStream<SensorReading> sensorReadingDataStream = sensorDs
                .partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int i) {
                return integer;
            }
        }, new KeySelector<SensorReading, Integer>() {
            @Override
            public Integer getKey(SensorReading sensorReading) throws Exception {
                return Integer.valueOf(sensorReading.getId().split("_")[1])%4;
//                return Integer.parseInt("2");
            }
        });

//        sensorReadingDataStream.print();

        env.execute();
    }
}
