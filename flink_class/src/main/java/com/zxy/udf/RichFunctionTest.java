package com.zxy.udf;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zxy
 * @create 2022-02-20 23:07
 */
public class RichFunctionTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        DataStreamSource<String> strDataStream = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        SingleOutputStreamOperator<Tuple2<Integer, String>> map = sensorDs.map(new MyRichMapFunction());


        map.print("map");

        env.execute();
    }

    private static class MyRichMapFunction extends RichMapFunction<SensorReading, Tuple2<Integer, String>> {

        @Override
        public Tuple2<Integer, String> map(SensorReading value) throws Exception {
            return new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), value.getId());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("my map open");
            // ????????????????????????????????????????????????????????????HDFS?????????
        }

        @Override
        public void close() throws Exception {
            System.out.println("my map close");
            // ?????????????????????????????????????????????HDFS?????????
        }
    }
}
