package com.zxy.state;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @author zxy
 * @create 2022-03-05 21:51
 * 实现简单的map统计-每个sensor    元素个数的状态管理
 */
public class StateTest2_KeyedState {

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
                .map(new MyKeyedState());

        countDS.print();

        env.execute();

    }

    public static class MyKeyedState extends RichMapFunction<SensorReading, Integer> {


        private ValueState<Integer> count;

        private ListState<Integer> listCount;

        private MapState<String, Integer> mapState;



        @Override
        public void open(Configuration parameters) throws Exception {
            //1.声明一个键控状态
            count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                    "sensor_count", Integer.class
            ));

            listCount = getRuntimeContext().getListState(new ListStateDescriptor<Integer>(
                    "list_state", Integer.class
            ));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer myValue = 0;
            //2.读取状态
            if(count.value() != null){
                myValue = count.value() + 1;
            }else {
                myValue = 1;
            }

            //3.对状态赋值
            count.update(myValue);

            return myValue;
        }

        @Override
        public void close() throws Exception {
            count.clear();
        }
    }

}
