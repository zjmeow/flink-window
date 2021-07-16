package com.zjmeow.window;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 类似于加上 distinct count 功能的 session windows
 * 一段时间没
 *
 * @author zjmeow
 */
public class CountTriggerWithTimeout extends Trigger<Object, GlobalWindow> {

    /**
     * 时间到了删除窗口
     */
    private Long sessionTimeout;
    /**
     * 窗口最大容纳元素
     */
    private int maxCount;
    /**
     * 最后一个元素到达时间
     */
    private ReducingStateDescriptor<Long> lastSeenDesc = new ReducingStateDescriptor<>("last-seen", new Sum(),
            Long.class);
    /**
     * 窗口中元素的个数
     */
    private ReducingStateDescriptor<Long> countDesc = new ReducingStateDescriptor<>("count", new Sum(), Long.class);

    public CountTriggerWithTimeout(int maxCount, long timeout) {
        this.maxCount = maxCount;
        this.sessionTimeout = timeout;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, GlobalWindow globalWindow, TriggerContext context)
            throws Exception {
        // 注册定时器，窗口超过一定时间没有元素到达则会触发
        context.registerEventTimeTimer(timestamp + sessionTimeout);
        // 以下代码用于删除上次的定时器
        ReducingState<Long> lastSeenState = context.getPartitionedState(this.lastSeenDesc);
        // 初始化 ReducingState
        lastSeenState.add(0L);
        long lastSeen = lastSeenState.get();
        if (lastSeen != 0) {
            context.deleteEventTimeTimer(lastSeen + sessionTimeout);
        }
        lastSeenState.add(timestamp - lastSeen);
        //以下代码用于更新窗口大小
        ReducingState<Long> count = context.getPartitionedState(this.countDesc);
        count.add(1L);
        long num = count.get();
        // 计数用于优化性能，没到阈值不fire，这个计数会在Process中也更新
        if (num >= maxCount) {
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    /**
     * 计时器触发，一段时间没有元素到达，删除窗口
     */
    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> lastSeenState = ctx.getPartitionedState(lastSeenDesc);
        Long lastSeen = lastSeenState.get();
        // 一段时间没元素到达直接删了
        if (time - lastSeen >= sessionTimeout) {
            System.out.println("CTX: " + ctx + " Firing Time " + time + " last seen " + lastSeen);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    /**
     * 删除计时器和存储的状态
     */
    @Override
    public void clear(GlobalWindow timeWindow, TriggerContext context) throws Exception {

        ReducingState<Long> lastSeenState = context.getPartitionedState(lastSeenDesc);
        if (lastSeenState.get() != 0) {
            context.deleteEventTimeTimer(lastSeenState.get() + sessionTimeout);
        }
        lastSeenState.clear();
    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, GlobalWindow timeWindow, TriggerContext triggerContext)
            throws Exception {
        return TriggerResult.FIRE;
    }

}

