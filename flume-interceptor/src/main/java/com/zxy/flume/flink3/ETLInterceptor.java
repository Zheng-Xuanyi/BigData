package com.zxy.flume.flink3;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * flume接入日志数据时的拦截器
 * 判断log的json格式是否合法
 * @author zxy
 * @create 2022-05-27 8:03
 */
public class ETLInterceptor implements Interceptor {


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1.获取事件数据，并转换为string
        byte[] eventBody = event.getBody();

        String log = new String(eventBody, StandardCharsets.UTF_8);

        //2. 判断该事件日志是否满足json格式
        if(JSONUtils.isJSONValidate(log)){
            //3.返回该事件
            return event;
        }else{
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        ArrayList<Event> eventList = new ArrayList<>();

        for (Event event : list) {
            Event e = intercept(event);
            if(e != null){
                eventList.add(e);
            }

        }
        return eventList;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }
        @Override
        public void configure(Context context) {

        }
    }

}
