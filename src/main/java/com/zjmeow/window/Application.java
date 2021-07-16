package com.zjmeow.window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义窗口，相当于count distinct time window 的结合
 * 参考 https://stackoverflow.com/questions/49783676/apache-flink-count-window-with-timeout
 *
 * @author zjmeow
 */
public class Application {

    private static final long WINDOW_TIME = Time.days(1).toMilliseconds();

    private static final int HOLD = 5;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<LoginEvent> dataStream = env.addSource(new EventSource());
        dataStream = dataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(LoginEvent event) {
                        return event.getTime();
                    }
                });
        dataStream.print();
        dataStream.keyBy(LoginEvent::getIp)
                .window(GlobalWindows.create())
                .trigger(new CountTriggerWithTimeout(HOLD, WINDOW_TIME))
                .evictor(new DistinctEvictor<>("userId", LoginEvent.class, HOLD, WINDOW_TIME))
                .process(new CountProcess<>(HOLD))
                .addSink(new SinkFunction<LoginEvent>() {
                    @Override
                    public void invoke(LoginEvent value, Context context) throws Exception {
                        System.out.println("blacklist:" + value);
                    }
                });

        env.execute();
    }


}
