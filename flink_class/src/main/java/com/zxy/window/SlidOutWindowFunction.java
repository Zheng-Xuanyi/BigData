package com.zxy.window;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author zxy
 * @create 2022-03-01 22:18
 */
public class SlidOutWindowFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        DataStreamSource<String> strDataStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        OutputTag<SensorReading> outputTag = new OutputTag<>("late");

        SingleOutputStreamOperator<SensorReading> sum = sensorDs.keyBy("id")
                .timeWindow(Time.seconds(5))
//                .trigger() //触发器
//                 .evictor() //移除器
//                .allowedLateness() //处理迟到的数据
                .sideOutputLateData(outputTag)
                .sum("temperature");

        sum.print();


        env.execute();
    }
}
