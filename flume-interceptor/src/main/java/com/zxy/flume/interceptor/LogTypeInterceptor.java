package com.zxy.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zxy
 * @create 2020-04-18 17:56
 */
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {
        
    }

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        String s = new String(body, Charset.forName("UTF-8"));

        Map<String, String> headers = event.getHeaders();

        if(s.contains("start")){
            headers.put("topic","topic_start");
        }else{
            headers.put("topic","topic_event");
        }
        
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : list) {
            Event intercept = intercept(event);

            interceptors.add(intercept);
        }
        
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
