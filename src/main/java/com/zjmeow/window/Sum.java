package com.zjmeow.window;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zjmeow
 */
public class Sum implements ReduceFunction<Long> {

    @Override
    public Long reduce(Long a, Long b) throws Exception {
        return a + b;
    }
}
