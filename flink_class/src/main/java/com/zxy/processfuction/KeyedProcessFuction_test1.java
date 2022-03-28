package com.zxy.processfuction;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zxy
 * @create 2022-03-08 8:34
 * 实现按key, 事件输入5s输出当前process time
 */
public class KeyedProcessFuction_test1 {

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

        OutputTag<Long> outputTag = new OutputTag<Long>("late_process_time") {
        };

        SingleOutputStreamOperator<SensorReading> res = sensorDS.keyBy("id")
                .process(new MyKeyedProcessFunction(outputTag));

        res.print("sensors");

        res.getSideOutput(outputTag).print("late_process_time");

        env.execute();

    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, SensorReading, SensorReading>{

        private OutputTag outputTag;

        public MyKeyedProcessFunction(OutputTag outputTag){
            this.outputTag = outputTag;
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            out.collect(value);

            ctx.timestamp();
            ctx.getCurrentKey();


            long timeRegist = ctx.timerService().currentProcessingTime() + 5000L;
            ctx.timerService().registerProcessingTimeTimer(timeRegist);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            ctx.output(outputTag, timestamp);

            ctx.getCurrentKey();
            ctx.timeDomain();
        }
    }
}
