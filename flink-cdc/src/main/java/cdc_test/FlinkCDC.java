package cdc_test;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zxy
 * @create 2022-03-27 23:57
 * flink cdc：使用stream api的方式
 *
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception{
        //1.获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.开启ck
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //3.通过Flink CDC 构建sourcefunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("034417")
                .databaseList("flink_test")
                .tableList("flink_test.cdc_test")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlSourceStream = env.addSource(sourceFunction);

        //4.打印数据
        mysqlSourceStream.print("flink_test.cdc_test");

        //5.启动任务
        env.execute("flink-cdc");

    }
}
