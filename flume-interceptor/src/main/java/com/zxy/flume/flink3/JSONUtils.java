package com.zxy.flume.flink3;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

/**
 * 判断flume的json是否合法
 * @author zxy
 * @create 2022-05-27 7:59
 */
public class JSONUtils {
    public static Boolean isJSONValidate(String log){
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }
}
