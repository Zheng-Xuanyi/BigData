package com.zxy.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.transform.stream.StreamSource;

/**
 * @author zxy
 * @create 2021-05-12 23:26
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        //1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //2.从文件中读取数据
        String input = "L:\\WorkSpace\\flink_class\\src\\main\\resources\\text.txt";
        DataStream<String> inputDataStream = env.readTextFile(input);

        //3.基于数据流进行转换操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WorldCount.MyPlatMapper())
                .keyBy(0)
                .sum(1);

        resultStream.print();  //此时没有任何输出——流处理需要有  触发

        //4.执行任务（区别于批处理）
        env.execute();
    }

}
