package com.zjmeow.window;

import com.google.common.collect.Iterables;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

/**
 * 攒够了去重然后丢给process判断
 *
 * @author zjmeow
 */
public class DistinctEvictor<T> implements Evictor<T, Window> {

    /**
     * 为了做的通用，利用反射和字段名来提取需要去重的字段
     */
    private String fieldName;
    private Class<T> clazz;
    private int threshold;
    private long timeout;

    public DistinctEvictor(String fieldName, Class<T> clazz, int threshold, long timeout) {

        this.fieldName = fieldName;
        this.clazz = clazz;
        this.threshold = threshold;
        this.timeout = timeout;
    }

    /**
     * 根据字段去重
     */
    @Override
    public void evictBefore(Iterable<TimestampedValue<T>> iterable, int size, Window window,
            EvictorContext context) {
        long watermark = context.getCurrentWatermark();
        Iterator<TimestampedValue<T>> iterator = iterable.iterator();
        Set<String> has = new HashSet<>();
        // 遍历窗口，删除超时元素和重复元素
        while (iterator.hasNext()) {
            TimestampedValue<T> timestampedValue = iterator.next();
            T t = timestampedValue.getValue();
            long time = timestampedValue.getTimestamp();
            // 删除已经超时的元素
            if (watermark - time > timeout) {
                iterator.remove();
                System.out.println("remove watermark - time:" + (watermark - time) / 3600000);
                continue;
            }
            String val = null;
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                val = (String) field.get(t);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                e.printStackTrace();
            }
            // 去重
            if (has.contains(val)) {
                iterator.remove();
            }
            has.add(val);
        }
    }

    /**
     * 在这里删除掉过期元素
     */
    @Override
    public void evictAfter(Iterable<TimestampedValue<T>> iterable, int size, Window window,
            EvictorContext context) {
        if (size >= threshold) {
            removeOld(iterable);
        }
    }

    /**
     * 删掉最早到来的
     * 注意，window里的元素是没有顺序的，也就是说如果想要删除最先到来的需要遍历整个window
     */
    private void removeOld(Iterable<TimestampedValue<T>> iterable) {
        Iterator<TimestampedValue<T>> iterator = iterable.iterator();
        long smallest = Long.MAX_VALUE;
        while (iterator.hasNext()) {
            TimestampedValue<T> t = iterator.next();
            smallest = Math.min(t.getTimestamp(), smallest);
        }
        Iterator<TimestampedValue<T>> removeIterator = iterable.iterator();
        while (removeIterator.hasNext()) {
            TimestampedValue<T> t = removeIterator.next();
            if (t.getTimestamp() == smallest) {
                removeIterator.remove();
                System.out.println("remove one element size:" + Iterables.size(iterable) + " element: " + t);
                return;
            }
        }

    }
}
