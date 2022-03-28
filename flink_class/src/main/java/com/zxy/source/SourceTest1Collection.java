package com.zxy.source;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author zxy
 * @create 2022-02-20 16:59
 */
public class SourceTest1Collection {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.创建数据源：source
        DataStreamSource<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 34);


        //3.数据处理过程

        //4.sink 输出
        dataStream.print().setParallelism(1);
        integerDataStream.print().setParallelism(2);

        //5.执行应用程序
        env.execute("sourceCollection-test");

    }

}
