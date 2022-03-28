package com.zxy.tableapi;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * @author zxy
 * @create 2022-03-09 23:30
 * table api 和 sql
 */
public class TableTest1_Example {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> sensorDs = stringDataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });

        // 3. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. 创建一张表: 基于流的方式
        Table table = tableEnv.fromDataStream(sensorDs);


        // 5. 调用table API进行转换操作
        Table resultTable = table.select("id, temperature")
                .where("id = 'sensor_1'");


        // 6. 执行SQL
        tableEnv.createTemporaryView("sensor", table);
        Table sqlQuery = tableEnv.sqlQuery("select id, temperature from sensor where id = 'sensor_1'");

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(sqlQuery, Row.class).print("sql");


        env.execute();

    }

}
