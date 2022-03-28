package com.zxy;



import org.junit.*;
import redis.clients.jedis.Jedis;

import java.util.Properties;
import java.util.Set;

/**
 * @author zxy
 * @create 2022-03-01 0:01
 */
public class jedis {
//    Properties properties = null;
//    Jedis jedis = null;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("host", "hadoop101");
        properties.setProperty("port", "6379");


        Jedis jedis = new Jedis(properties.getProperty("host"), Integer.valueOf(properties.getProperty("port")));
        String pong = jedis.ping();

        System.out.println("链接成功：" + pong);

        jedis.close();
    }

//    @Before
//    public void getJedis(){
//        properties.setProperty("host", "hadoop101");
//        properties.setProperty("port", "6379");
//
//        jedis =  new Jedis(properties.getProperty("host"), Integer.valueOf(properties.getProperty("port")));
//
//        String pong = jedis.ping();
//
//        System.out.println("链接成功：" + pong);
//    }

    @Test
    public void testKey() {
        Properties properties = new Properties();
        properties.setProperty("host", "hadoop101");
        properties.setProperty("port", "6379");


        Jedis jedis = new Jedis(properties.getProperty("host"), Integer.valueOf(properties.getProperty("port")));
        String pong = jedis.ping();

        System.out.println("链接成功：" + pong);

        Long setnx = jedis.setnx("apitest1", "ok");
        System.out.println(setnx);

        Set<String> keys = jedis.keys("*");
        for (String key : keys
             ) {
            System.out.println(key);
        }

        System.out.println(jedis.ttl("apitest1"));
        System.out.println(jedis.get("apitest1"));

        jedis.close();
    }
//
//    @After
//    public void closeJedis(){
//        jedis.close();
//    }
}
