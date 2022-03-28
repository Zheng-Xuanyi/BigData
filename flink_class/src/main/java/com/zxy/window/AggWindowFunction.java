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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zxy
 * @create 2022-03-01 22:18
 */
public class AggWindowFunction {
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

         sensorDs.keyBy("id")
                .countWindow(5)
                 .reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading v1, SensorReading v2) throws Exception {
                return v1.getTemperature() > v2.getTemperature() ? v2 : v1 ;
            }
        });


        sensorDs.keyBy("id")
                .countWindow(5)
                .aggregate(new AggregateFunction<SensorReading, Tuple3<String, Integer, Double>, Tuple2<String, Double>>() {

                    @Override
                    public Tuple3<String, Integer, Double> createAccumulator() {
                        return new Tuple3<String, Integer, Double>("", 0 ,0.0);
                    }

                    @Override
                    public Tuple3<String, Integer, Double> add(SensorReading sensorReading, Tuple3<String, Integer, Double> acc) {
                        return new Tuple3<String, Integer, Double>(sensorReading.getId(), acc.f1 + 1 , acc.f2 + sensorReading.getTemperature());
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Integer, Double> acc) {
                        return new Tuple2<String, Double>(acc.f0, acc.f2/acc.f1);
                    }

                    @Override
                    public Tuple3<String, Integer, Double> merge(Tuple3<String, Integer, Double> acc, Tuple3<String, Integer, Double> acc1) {
                        return new Tuple3<String, Integer, Double>(acc.f0, acc.f1 + acc1.f1, acc.f2 +acc1.f2);
                    }
                })
        .print();

        env.execute();
    }
}
