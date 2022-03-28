package com.zxy.processfuction;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zxy
 * @create 2022-03-08 8:34
 * 温度传感器监控：温度值在10s内，连续上升，则报警
 */
public class KeyedProcessFuctionApp_test2 {

    public static void main(String[] args) throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });


        SingleOutputStreamOperator<String> process = sensorDS.keyBy("id")
                .process(new MyKeyedProcessFunction(5));

        process.print("sensor-warning");


        env.execute();

    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, SensorReading, String>{

        //配置报警时间间隔
        private Integer secondsGap;

        public MyKeyedProcessFunction(int secondsGap){
            this.secondsGap = secondsGap;
        }

        //设置状态：保存传感器前一个温度值
        private ValueState<Double> lastTemp;
        //设置状态：当前定时器时间戳
        private ValueState<Long> timeTs;

        //声明一个键控状态
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
            timeTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeTs", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            //取出状态
            Long time1 = timeTs.value();
            Double temp1 = lastTemp.value();

            //更新传感器温度值
            lastTemp.update(value.getTemperature());

            //判断是否注册触发器
            if(temp1 == null || (value.getTemperature() > temp1 && time1 == null)){
                long ts = ctx.timerService().currentProcessingTime() + secondsGap * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timeTs.update(ts);
            }else if(value.getTemperature() < temp1 && time1 != null){
                ctx.timerService().deleteProcessingTimeTimer(time1);
                timeTs.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect("传感器" + ctx.getCurrentKey() + "的温度连续" + secondsGap + "秒上升");

            timeTs.clear();
        }
    }
}
