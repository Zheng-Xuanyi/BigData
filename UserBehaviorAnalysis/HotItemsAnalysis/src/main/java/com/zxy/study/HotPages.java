package com.zxy.study;

import com.sun.org.apache.bcel.internal.generic.NEW;
import com.zxy.study.Pojo.ApacheLogEvent;
import com.zxy.study.Pojo.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author zxy
 * @create 2022-03-16 7:19
 */
public class HotPages {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境、设置并行度及事件时间
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.获取数据源
//        URL resource = HotPages.class.getResource("/apache.log");
//        System.out.println(resource); // file:/L:/WorkSpace/UserBehaviorAnalysis/HotItemsAnalysis/target/classes/apache.log
//        System.out.println(resource.getFile()); // /L:/WorkSpace/UserBehaviorAnalysis/HotItemsAnalysis/target/classes/apache.log
//        DataStreamSource<String> stringDataStream = env.readTextFile(resource.getPath());
        DataStream<String> stringDataStream = env.socketTextStream("hadoop101", 7777).uid("socket-source").name("a");

        //3.转换为POJO类,设置水位线
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = stringDataStream.map(line -> {
                String[] fields = line.split(" ");
                SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                long time = dateFormat.parse(fields[3]).getTime();//17/05/2015:10:05:03

                return new ApacheLogEvent(fields[0], fields[1], time, fields[5], fields[6]);
            }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                @Override
                public long extractTimestamp(ApacheLogEvent element) {
                    return element.getTimestamp();
                }
            });


        dataStream.print("data");

        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
        };

        SingleOutputStreamOperator<PageViewCount> windowAggDataStream = dataStream.filter(line -> "GET".equals(line.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(new KeySelector<ApacheLogEvent, String>() {
                    @Override
                    public String getKey(ApacheLogEvent apacheLogEvent) throws Exception {
                        return apacheLogEvent.getUrl();
                    }
                })
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowAggDataStream.print("agg");
        windowAggDataStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggDataStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print("res");


        env.execute();
    }
    // 自定义预聚合函数
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String>{

        private int topSize;

        public TopNHotPages(int topSize) {
            this.topSize = topSize;
        }

        //定义状态
//        ListState<PageViewCount> pageViewCountListState;
        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-count-list", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
        }

        @Override
        public void close() throws Exception {
            //验证聚合之后是否会自动注销状态：不会
            pageViewCountMapState.clear();
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            //同窗口数据，来一条收入状态一条; 同时注册窗口结束时间的触发器
//            pageViewCountListState.add(value);
            pageViewCountMapState.put(value.getUrl(), value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            //为了能释放状态，最晚延迟时间后注销状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if( timestamp == ctx.getCurrentKey() + 60 * 1000L ){
                pageViewCountMapState.clear();
                return;
            }
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());

//            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());

//            pageViewCounts.sort(new Comparator<PageViewCount>() {
//                @Override
//                public int compare(PageViewCount o1, PageViewCount o2) {
//                     if(o1.getCount() > o2.getCount())
//                        return -1;
//                    else if(o1.getCount() < o2.getCount())
//                        return 1;
//                    else
//                        return 0;
//                }
//            });

            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if(o1.getValue() > o2.getValue())
                        return -1;
                    else if(o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });

            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
//                PageViewCount pageViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());

//            pageViewCountListState.clear();


        }
    }


}
