package com.zxy.state;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zxy
 * @create 2022-03-05 21:51
 * 状态后端
 * 容错处理
 */
public class StateTest4_StateBackend {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //1.状态后端配置
//        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));



        // 2. 检查点配置: 时间间隔，周期性的启动检查点的保存
        env.enableCheckpointing(1000);
        // 高级选项
        env.getCheckpointConfig().setCheckpointInterval(1000);  //设置时间间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //exactly-once vs. at-least-once
        env.getCheckpointConfig().setCheckpointTimeout(60000);   //检查点超时时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); //前一次ck保存成功 与 下一次ck触发时间间隔最小值
        //默认为1 ， 当前同时存在的ck只能有2个
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2); //当前的ck没有处理完，下一个ck已经开始了
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false); //是否优先使用ck恢复，默认选择最新的sk或ck
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0); //容忍ck失败多少次， 默认0


        // 3. 重启策略配置
        // 固定延迟重启（隔一段时间尝试重启一次）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,  // 尝试重启次数
                100000 // 尝试重启的时间间隔，也可org.apache.flink.api.common.time.Time
        ));

        //10分钟内重启3次，每次重启间隔1分钟
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.minutes(10),
                Time.minutes(1)
        ));

        DataStreamSource<String> strDataStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDs = strDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0].trim(), Long.valueOf(split[1].trim()), Double.valueOf(split[2].trim()));
            }
        });


        SingleOutputStreamOperator<Tuple3<String, Double, Double>> warning = sensorDs.keyBy("id")
                .flatMap(new MyFlatMapFunction(10.0));

        warning.print();

        env.execute();

    }

    //自定义mapfuncion
    public static class MyFlatMapFunction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private ValueState<Double> lastTemp;

        private double level = 0.0;

        public MyFlatMapFunction(double level){
            this.level = Math.abs(level);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //声明键控状态
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                    "sensor_temp", Double.class
            ));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            //获取键控状态值：如果为空则赋当前温度值
            double nowTemp = sensorReading.getTemperature();

            if(lastTemp.value() != null && Math.abs(nowTemp - lastTemp.value()) > level){
                collector.collect(new Tuple3<>(sensorReading.getId(), lastTemp.value(), nowTemp));
            }

            lastTemp.update(nowTemp);
        }


        @Override
        public void close() throws Exception {
            lastTemp.clear();
        }
    }
}
