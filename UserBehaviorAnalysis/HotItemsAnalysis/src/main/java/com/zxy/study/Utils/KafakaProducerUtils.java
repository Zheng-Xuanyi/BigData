package com.zxy.study.Utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * @author zxy
 * @create 2022-03-14 22:43
 */
public class KafakaProducerUtils {

    public static void main(String[] args) throws Exception{
        //基本类的规范名
        String canonicalName = KafakaProducerUtils.class.getCanonicalName();
        System.out.println(canonicalName);
        //使用反射，提取resource中的文件地址
        URL resource = KafakaProducerUtils.class.getClassLoader().getResource("/");
        System.out.println(resource);

        URL resource1 = KafakaProducerUtils.class.getResource("/UserBehavior.csv");
        System.out.println(resource1);

        /*输出结果
        * com.zxy.study.Utils.KafakaProducerUtils
        * file:/L:/WorkSpace/UserBehaviorAnalysis/HotItemsAnalysis/target/classes/UserBehavior.csv
        *
        * */
//        String path = "L:\\WorkSpace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv";
//        String path = resource1.getPath();
//        String topic = "hotitems";
//        fileTxtToKafka(path, topic);
    }

    public static void fileTxtToKafka(String path,String topic) throws IOException {
        //1.kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop101:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2.定义一个kafka producer：K V ~ topic, value
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //读取文本，传入kafka
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line;
        while ((line = bufferedReader.readLine()) != null ){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            //3.用producer发送数据
            producer.send(producerRecord);
        }


        //4.关闭 producer
        producer.close();
    }


}
