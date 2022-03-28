package com.zxy.tableUdf;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author zxy
 * @create 2022-03-12 23:50
 */
public class Table_UDF {

    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        tableEnvironment.connect(new FileSystem().path("L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts", DataTypes.BIGINT())
                    .field("tm", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");


        TempHighWarning tempHighWarning = new TempHighWarning(30.0);

        tableEnvironment.registerFunction("temp_if_warning", tempHighWarning);

        //3.使用table api
        Table inputTable = tableEnvironment.from("inputTable");
        Table result = inputTable.select("id, ts, tm, temp_if_warning(tm)");
//                .where("id === 'sensor_1'");


        tableEnvironment.toAppendStream(result, Row.class).print("result");

        env.execute();
    }

    // 实现自定义的ScalarFunction——标量函数
    public static class TempHighWarning extends ScalarFunction{

        private double factor = 30;

        public TempHighWarning(double factor){
            this.factor = factor;
        }


        public String eval(double temp){
            return temp>30 ? "high" : "low";
        }
    }

}
