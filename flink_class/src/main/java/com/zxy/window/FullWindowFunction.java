package com.zxy.window;

import com.zxy.source.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author zxy
 * @create 2022-03-01 22:18
 */
public class FullWindowFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> strDataStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = sensorDs.keyBy("id")
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        int size = IteratorUtils.toList(iterable.iterator()).size();
                        String field = tuple.getField(0);
                        long end = timeWindow.getEnd();

                        collector.collect(new Tuple3(field, end, size));
                    }
                });

//        apply.print();

        sensorDs.keyBy("id").timeWindow(Time.seconds(10)).process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<SensorReading> iterable, Collector<Object> collector) throws Exception {
                int size = IteratorUtils.toList(iterable.iterator()).size();
                String field = tuple.getField(0);
                long end = context.window().getEnd();

                collector.collect(new Tuple3(field, end, size));
            }

            @Override
            public void clear(Context context) throws Exception {
                super.clear(context);
            }
        }).print();

        env.execute();
    }
}
