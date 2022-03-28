package com.zxy.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zxy
 * @create 2022-02-20 23:07
 */
public class TransformTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> strDataStream = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\text.txt");

        DataStream<Integer> map = strDataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        DataStream<String> flatmap = strDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String str : split) {
                    collector.collect(str);
                }
            }
        });

        DataStream<String> filter = strDataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.length() == 3;
            }
        });

        map.print("map");
        flatmap.print("flatmap");
        filter.print("filter");

        env.execute();
    }
}
