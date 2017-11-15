package com.github.danielwegener.logback.kafka.delivery;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author heyc
 */
public class DeliveryStrategySupport {

    public static DeliveryStrategySupport INSTANCE = new DeliveryStrategySupport();

    /**
     * 连续失败次数
     */
    private AtomicInteger failCount = new AtomicInteger(0);

    /**
     *  连续失败${failOffCount}后关闭
     */
    private int failOffCount = 3;

    /**
     * 上次失败关闭时间
     */
    private volatile long lastFailOffTime = 0;

    /**
     *  失败恢复时间间隔
     */
    private int failOverTime = 30000;

    public AtomicInteger getFailCount() {
        return failCount;
    }

    public void setFailCount(AtomicInteger failCount) {
        this.failCount = failCount;
    }

    public int getFailOffCount() {
        return failOffCount;
    }

    public void setFailOffCount(int failOffCount) {
        this.failOffCount = failOffCount;
    }

    public long getLastFailOffTime() {
        return lastFailOffTime;
    }

    public void setLastFailOffTime(long lastFailOffTime) {
        this.lastFailOffTime = lastFailOffTime;
    }

    public int getFailOverTime() {
        return failOverTime;
    }

    public void setFailOverTime(int failOverTime) {
        this.failOverTime = failOverTime;
    }

}
