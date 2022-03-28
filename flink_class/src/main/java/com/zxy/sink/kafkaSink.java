package com.zxy.sink;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author zxy
 * @create 2022-03-01 8:42
 */
public class kafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDs = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        SingleOutputStreamOperator<String> sensorDs = stringDs.map( (s) -> {
                    String[] split = s.split(",");
                    return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim())).toString();
                }
        );

        DataStreamSink<String> test1 = sensorDs.addSink(new FlinkKafkaProducer011<String>("hadoop101:9092", "test1", new SimpleStringSchema()));

        env.execute();
    }
}
