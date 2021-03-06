package cdc_test;

import cdc_test.pojo.UserInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zxy
 * @create 2022-03-27 23:57
 * flink cdc：使用flink sql的方式
 *
 */
public class FlinkSqlCDC {
    public static void main(String[] args) throws Exception{
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用FLINKSQL DDL模式构建CDC 表
        tableEnv.executeSql("CREATE TABLE user_info ( " +
                " id int primary key, " +
                " name STRING, " +
                " sex STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = 'hadoop101', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '034417', " +
                " 'database-name' = 'flink_test', " +
                " 'table-name' = 'cdc_test' " +
                ")");

        //3.查询数据并转换为流输出
        Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print("row-data");

        // retractStream 可以转为 POJO类：对应两边schema信息必须一致
        DataStream<Tuple2<Boolean, UserInfo>> userInfo = tableEnv.toRetractStream(table, UserInfo.class);
        userInfo.print("user-pojo");

        //4.启动
        env.execute("FlinkSQLCDC");
    }
}
