package com.zxy.source;

import com.zxy.source.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author zxy
 * @create 2022-02-20 18:18
 */
public class SourceTest4_UDF {

    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.source 创建数据源：从文件读取数据
        DataStreamSource<SensorReading> strDataStream = env.addSource(new MySource());

        //3.数据处理过程

        //4.sink 输出
        strDataStream.print();

        //5.执行应用程序
        env.execute("MySource-test");
    }

    //实现自定义 SourceFunction
    public static class MySource implements SourceFunction<SensorReading>{
        //定义一个标志位：用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //定义一个产生随机温度的10个传感器值的hashmap
            Random random = new Random();

            HashMap<String, Double> sensorTem = new HashMap<String, Double>();
            for (int i = 1; i<= 10; i++){
                sensorTem.put("sensor_" + i, random.nextGaussian()*10 + 60);
            }

            while (running){
                for ( String key : sensorTem.keySet()){
                    double newTem = sensorTem.get(key) +random.nextGaussian();
                    sensorTem.put(key, newTem);

                    sourceContext.collect(new SensorReading(key, System.currentTimeMillis(), newTem));
                }

                Thread.sleep(1000L);

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
