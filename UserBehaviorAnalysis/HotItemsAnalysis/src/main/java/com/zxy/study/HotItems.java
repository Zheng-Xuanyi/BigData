package com.zxy.study;

import com.zxy.study.Pojo.ItemViewCount;
import com.zxy.study.Pojo.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * @author zxy
 * @create 2022-03-14 23:41
 */
public class HotItems {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        //3.读取数据转换成POJO类
//        String path = "L:\\WorkSpace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv";
//        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop101:9092");
        properties.setProperty("group.id", "consumer");
        //指定了SimpleStringSchema可以不写
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            //earliest： 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
            //latest： 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
            //none： topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));

        DataStream<UserBehavior> dataStream =  dataStreamSource.filter(line -> "pv".equals(line.split(",")[3]))
                .map(line -> new UserBehavior(
                                Long.valueOf(line.split(",")[0]),
                                Long.valueOf(line.split(",")[1]),
                                Integer.valueOf(line.split(",")[2]),
                                line.split(",")[3],
                                Long.valueOf(line.split(",")[4])
                        )
                );

        //4.设置事件时间、水位线
        SingleOutputStreamOperator<UserBehavior> itemDataStream = dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //5.分组、开窗、按商品品类聚合，并赋窗口信息
        SingleOutputStreamOperator<ItemViewCount> itemViewCountDS = itemDataStream
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new ItemWindowResult());

//        itemDataStream.print();

        //6. 收集同一窗口的所有商品count数据，排序输出top n
        SingleOutputStreamOperator<String> end = itemViewCountDS
                .keyBy("windowEnd")
                .process(new TopNHotItems(5));


        end.print();

        env.execute("hot items analysis");
    }

    //自定义聚合函数：求每个itemId的累加计数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    //自定义窗口函数：窗口闭合后给累加值赋窗口结束时间
    public static class ItemWindowResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long aggResult = input.iterator().next();
            long end = window.getEnd();

            out.collect(new ItemViewCount(end, aggResult, itemId));
        }
    }

    //自定义 topN
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String>{

        private int topn;
        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        public TopNHotItems(int topn) {
            this.topn = topn;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item_view_count"
                                        , ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //定义触发器：排序list, 数出结果string
            String windowEnd = new Timestamp(timestamp - 10).toString();

            ArrayList<ItemViewCount> itemArrayList = Lists.newArrayList(itemViewCountListState.get().iterator());
            //排序
            itemArrayList.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });

            StringBuffer resultString = new StringBuffer();
            resultString.append("================================\n")
                    .append("窗口："+ windowEnd + " 的输出结果：\n");

            for (int i = 0; i < Math.min(itemArrayList.size(), topn); i++) {
                ItemViewCount itemViewCount = itemArrayList.get(i);
                resultString.append("    top")
                    .append(i)
                    .append(":")
                    .append(itemViewCount)
                    .append("\n");
            }

            resultString.append("================================\n\n");

            Thread.sleep(1000L);

            out.collect(resultString.toString());

        }

        @Override
        public void close() throws Exception {
            itemViewCountListState.clear();
        }
    }
}
