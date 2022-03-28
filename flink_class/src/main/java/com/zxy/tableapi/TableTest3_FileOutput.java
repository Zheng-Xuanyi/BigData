package com.zxy.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author zxy
 * @create 2022-03-09 23:30
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception{
       //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 表的创建：连接外部系统，读取数据
        // 读取文件
        tableEnv.connect(new FileSystem().path("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts", DataTypes.BIGINT())
                    .field("tm",DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table inputTable = tableEnv.from("inputTable");
        Table result = inputTable.select("id, ts, tm")
                .where("id = 'sensor_1'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        Table sqlResult = tableEnv.sqlQuery("select id, ts, tm from inputTable");
        Table aggResult = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id");

        // 4. 输出到文件
        // 连接外部文件注册输出表
        tableEnv.connect(new FileSystem().path("L:\\WorkSpace\\flink_class\\src\\main\\resources\\out.txt"))
            .withFormat(new Csv())
            .withSchema(new Schema()
                    .field("out_id", DataTypes.STRING())
//                    .field("out_ts", DataTypes.BIGINT())
//                    .field("out_tm",DataTypes.DOUBLE())
                    .field("out_cnt", DataTypes.BIGINT())
            ).createTemporaryTable("outputTable");

//        result.insertInto("outputTable");
//        sqlResult.insertInto("outputTable");
//        aggResult.insertInto("outputTable");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(aggResult, Row.class);
        tuple2DataStream
                .print();

        env.execute();
    }
}
