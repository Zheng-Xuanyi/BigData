package com.zxy.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zxy
 * @create 2021-05-12 23:26
 */
public class LinuxStreamWordCount {

    public static void main(String[] args) throws Exception {

        //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);
        // 用ParameterTool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = inputDataStream.flatMap(new WorldCount.MyPlatMapper())
                .keyBy(0)
                .sum(1);

        sum.print();

        env.execute();
    }

}
