package com.zxy.event_time;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @author zxy
 * @create 2022-03-05 10:29
 */
public class WaterMark_Test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 每隔5秒产生一个watermark: 默认200毫秒
        env.getConfig().setAutoWatermarkInterval(5000);


        DataStreamSource<String> socketDS = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDS = socketDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        //Event Time的使用一定要指定数据源中的时间戳， 设置水位线为两秒
        SingleOutputStreamOperator<SensorReading> eventTimeDS = sensorDS.assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading s) {
                return s.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<SensorReading> minTemp = eventTimeDS.keyBy("id")
                .timeWindow(Time.seconds(10))
                .minBy("temperature");

        minTemp.print("minTemp");

        env.execute();
    }
}
