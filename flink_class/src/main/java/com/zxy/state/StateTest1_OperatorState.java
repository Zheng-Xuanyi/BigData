package com.zxy.state;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zxy
 * @create 2022-03-05 21:51
 * 实现简单的map统计元素个数的状态管理
 */
public class StateTest1_OperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> strDataStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });


        SingleOutputStreamOperator<Integer> countDS = sensorDs.keyBy("id")
                .map(new MyMapFunction());

        countDS.print();

        env.execute();

    }

    //自定义mapfuncion
    public static class MyMapFunction implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        // 定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }


        // 返回状态快照
        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        // 从状态备份恢复状态
        @Override
        public void restoreState(List<Integer> list) throws Exception {
            for (Integer num : list) {
                count += num;
            }
        }
    }
}
