package com.zxy.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author zxy
 * @create 2022-02-20 16:59
 */
public class SourceTest3Kafka {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.source 创建数据源：kafka —— pom需要引入kafka连接器的依赖
        // addSource 函数需实现 SourceFunction 接口
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop101:9092");
        properties.setProperty("group.id", "consumer-group");
        //kafka反序列化类
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> strDataStream = env.addSource(new FlinkKafkaConsumer011<String>(
                "test1", new SimpleStringSchema(), properties
        ));

        //3.数据处理过程

        //4.sink 输出
        strDataStream.print("sensor");

        //5.执行应用程序
        env.execute("sourceKafka-test");

    }

}
