package com.zxy.study.Pojo;

/**
 * @author zxy
 * @create 2022-03-14 23:38
 */
public class ItemViewCount {
    private Long windowEnd;
    private Long count;
    private Long itemId;

    public ItemViewCount(Long windowEnd, Long count, Long itemId) {
        this.windowEnd = windowEnd;
        this.count = count;
        this.itemId = itemId;
    }

    public ItemViewCount() {
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                ", itemId=" + itemId +
                '}';
    }
}
