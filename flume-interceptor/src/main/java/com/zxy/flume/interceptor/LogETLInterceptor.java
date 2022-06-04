package com.zxy.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zxy
 * @create 2020-04-18 15:43
 */
public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 作用：数据清洗：将不满足条件的event排除
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {

        //1.获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        //2.判断数据类型，并向header中赋值
        if(log.contains("start")){
            if(LogUtils.validateStart(log)){
                return event;
            }
        } else {
            if(LogUtils.validateEvent(log)){
                return event;
            }
        }

        //3.返回校检结果
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        ArrayList<Event> interceptor = new ArrayList<>();

        for (Event event : list) {
            Event intercept1 = intercept(event);
            if (intercept1 != null){
                interceptor.add(intercept1);
            }
        }


        return interceptor;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
