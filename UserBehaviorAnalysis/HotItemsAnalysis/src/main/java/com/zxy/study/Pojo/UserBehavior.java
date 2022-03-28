package com.zxy.study.Pojo;

/**
 * @author zxy
 * @create 2022-03-14 0:24
 */
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer catagoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior(Long userId, Long itemId, Integer catagoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.catagoryId = catagoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public UserBehavior() {
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Integer getCatagoryId() {
        return catagoryId;
    }

    public void setCatagoryId(Integer catagoryId) {
        this.catagoryId = catagoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", catagoryId=" + catagoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
