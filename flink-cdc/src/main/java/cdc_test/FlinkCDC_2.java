package cdc_test;

import cdc_test.func.CustomerDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zxy
 * @create 2022-03-27 23:57
 * flink cdc：使用stream api的方式
 * 自定义 反序列化器
 *
 */
public class FlinkCDC_2 {
    public static void main(String[] args) throws Exception{
        //1.获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.开启ck
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new FsStateBackend("hdfs://192.168.8.101:8020/mydata"));

        // 重启策略配置
        // 固定延迟重启（隔一段时间尝试重启一次）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,  // 尝试重启次数
                100000 // 尝试重启的时间间隔，也可org.apache.flink.api.common.time.Time
        ));

        //3.通过Flink CDC 构建sourcefunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("034417")
                .databaseList("flink_test")  //只写库名：则当前库下所有表的binlog都将收集至flink
                .tableList("flink_test.cdc_test")
                .deserializer(new CustomerDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlSourceStream = env.addSource(sourceFunction);

        //4.打印数据
        mysqlSourceStream.print("flink_test.cdc_test");

        //5.启动任务
        env.execute("flink-cdc");

    }
}
