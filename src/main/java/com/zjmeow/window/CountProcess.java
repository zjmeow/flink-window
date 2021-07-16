package com.zjmeow.window;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * 元素
 *
 * @author zjmeow
 */
public class CountProcess<T extends BaseEvent, W extends Window> extends
        ProcessWindowFunction<T, T, String, W> {

    private int threshold;
    private ReducingStateDescriptor<Long> count = new ReducingStateDescriptor<>("count", new Sum(), Long.class);

    public CountProcess(int threshold) {
        this.threshold = threshold;
    }


    @Override
    public void process(String key, Context context, Iterable<T> iterable, Collector<T> collector) throws Exception {
        // todo 优化 size方法会去遍历一次，这里可以少遍历几次
        int size = Iterables.size(iterable);
        ReducingState<Long> windowSize = context.windowState().getReducingState(count);
        windowSize.add(size - windowSize.get());
        if (size < threshold) {
            return;
        }
        // 需要遍历一遍确认没有输出过，以免重复输出
        for (T t : iterable) {
            if (!t.isOutputted()) {
                collector.collect(t);
                t.setOutputted(true);
            }
        }
        // 因为会去处理下，这里提前给减1了
        windowSize.add(-1L);

    }


}
