package com.zjmeow.window;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 测试数据源
 *
 * @author zjmeow
 */
public class EventSource implements SourceFunction<LoginEvent> {

    private boolean running = true;
    public static final int GAP_TIME = 500;

    @Override
    public void run(SourceContext<LoginEvent> ctx) throws Exception {
        while (running) {
            LoginEvent event = new LoginEvent();
            event.setIp("127.0.0.1");
            // 生成用户id
            event.setUserId(System.currentTimeMillis() + "");
            ctx.collect(event);
            Thread.sleep(GAP_TIME);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
