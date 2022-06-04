package com.zxy.flume.flink3;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zxy
 * @create 2022-05-27 20:20
 */
public class TimeStampInterceptor implements Interceptor {


    private ArrayList<Event> events = new ArrayList<>();


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //{"common":{"ar":"310000","ba":"Redmi","ch":"web","is_new":"1","md":"Redmi k30","mid":"mid_498482","os":"Android 11.0","uid":"936","vc":"v2.1.134"},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","order":1,"pos_id":1},{"display_type":"promotion","item":"35","item_type":"sku_id","order":2,"pos_id":5},{"display_type":"query","item":"34","item_type":"sku_id","order":3,"pos_id":1},{"display_type":"query","item":"23","item_type":"sku_id","order":4,"pos_id":1},{"display_type":"query","item":"22","item_type":"sku_id","order":5,"pos_id":5}],"page":{"during_time":6916,"page_id":"home"},"ts":1653307879000}


        byte[] body = event.getBody();

        String log = new String(body, StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        String ts = jsonObject.getString("ts");

        event.getHeaders().put("timestamp", ts);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        events.clear();
        for (Event event : list) {
            events.add(intercept(event));
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
