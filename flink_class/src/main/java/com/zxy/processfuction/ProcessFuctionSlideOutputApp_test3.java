package com.zxy.processfuction;

import akka.japi.tuple.Tuple3;
import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import redis.clients.jedis.Tuple;

/**
 * @author zxy
 * @create 2022-03-08 8:34
 * 监控传感器温度值，将温度值低于30度的数据输出到side output。
 */
public class ProcessFuctionSlideOutputApp_test3 {

    public static void main(String[] args) throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        OutputTag<SensorReading> high = new OutputTag<SensorReading>("high") {
        };

        OutputTag<Tuple3<String, Long, Double>> low = new OutputTag<Tuple3<String, Long, Double>>("low") {
        };

        SingleOutputStreamOperator<SensorReading> process = sensorDS.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() > 30) {
                    ctx.output(high, value);
                } else {
                    ctx.output(low, new Tuple3<>(value.getId(), value.getTimestamp(), value.getTemperature()));
                }
            }
        });

        DataStream<SensorReading> sideOutput1 = process.getSideOutput(high);
        sideOutput1.print("high");

        DataStream<Tuple3<String, Long, Double>> sideOutput2 = process.getSideOutput(low);
        sideOutput2.print("low");

        SingleOutputStreamOperator<String> process1 = sideOutput1.connect(sideOutput2).process(new CoProcessFunction<SensorReading, Tuple3<String, Long, Double>, String>() {
            @Override
            public void processElement1(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }

            @Override
            public void processElement2(Tuple3<String, Long, Double> value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }
        });

        process1.print("all");

        env.execute();

    }

}
