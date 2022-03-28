package com.zxy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zxy
 * @create 2021-05-11 23:53
 */
public class WorldCount {
    public static void main(String[] args) throws Exception {
        //1.创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文件中读取数据
        String input = "L:\\WorkSpace\\flink_class\\src\\main\\resources\\text.txt";
        DataSet<String> inputDataSet = env.readTextFile(input);

        //3.对数据集进行处理： flatMap参数是一个FlatMapFunction接口，接口里实现一个无返回参数的flatMap函数
        DataSet<Tuple2<String, Integer>> res = inputDataSet.flatMap(new MyPlatMapper())
                .groupBy(0)  //按照第一个位置分组
                .sum(1);       //将第二个位置上的数据求和

        res.print();

    }


    //自定义类：实现FlatMapFunction接口——重写flatMap方法
    public static class MyPlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words = value.split(" ");

            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }

        }
    }

}




