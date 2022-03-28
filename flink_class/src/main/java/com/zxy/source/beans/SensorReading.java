package com.zxy.source.beans;

/**
 * @author zxy
 * @create 2022-02-20 17:02
 * 传感器类
 */
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;

    //创建对应的空参构造方法
    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

    public static void main(String[] args) {
        SensorReading sensorTest = new SensorReading("sensorTest", 1547718211L, 30.2);

        System.out.println(sensorTest);
    }
}
