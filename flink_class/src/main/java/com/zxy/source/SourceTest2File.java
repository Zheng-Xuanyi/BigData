package com.zxy.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zxy
 * @create 2022-02-20 16:59
 */
public class SourceTest2File {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.source 创建数据源：从文件读取数据
        String inputPath = "L:\\WorkSpace\\flink_class\\src\\main\\resources\\source_test.txt";
        DataStreamSource<String> strDataStream = env.readTextFile(inputPath);

        //3.数据处理过程

        //4.sink 输出
        strDataStream.print("sensor");

        //5.执行应用程序
        env.execute("sourceFile-test");

    }

}
