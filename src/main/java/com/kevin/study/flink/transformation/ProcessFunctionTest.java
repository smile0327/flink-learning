package com.kevin.study.flink.transformation;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import com.kevin.study.flink.StationLog;

import javax.annotation.Nullable;

import java.sql.Timestamp;

/**
 * @Auther: kevin
 * @Description:  Flink定时器测试
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 16:21 2020/5/12
 * @ProjectName: Flink-SXT
 */
public class ProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> input = env.socketTextStream("10.172.246.227", 8888);

        DataStream<StationLog> logStream = input.map(line -> {
            String[] split = line.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
        }).assignTimestampsAndWatermarks(new MyWatermarks());

        logStream.keyBy("sid")
                .process(new MonitorDeletedDataFunc())
                .print();

        env.execute("timeTestTask");

    }
}

class MyWatermarks implements AssignerWithPeriodicWatermarks<StationLog>{

    Long eventTime = 0L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(eventTime);
    }

    @Override
    public long extractTimestamp(StationLog element, long previousElementTimestamp) {
        eventTime = element.getCallTime();
        return element.getCallTime();
    }
}

class MonitorDeletedDataFunc extends KeyedProcessFunction<Tuple , StationLog , String>{

    ValueState<Long> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time" , Long.class));
    }

    @Override
    public void processElement(StationLog value, Context ctx, Collector<String> out) throws Exception {
        Long time = state.value();
        /*
            每次接收到消息后先清空定时器，将当前数据的time写入到state，然后注册定时器，
            如果该key的数据不再接收了，那么定时器就会在将来某个时刻触发。
         */
        //清空定时器
        if (time != null) {
            ctx.timerService().deleteEventTimeTimer(time);
        }
        Long nowTime = value.getCallTime();
        //5s后触发
        Long onTime = nowTime + 5 * 1000;
        System.out.println("线程ID:" + Thread.currentThread().getId() + ",key:" + value.getSid() + " 注册定时器.当前时间：" + new Timestamp(nowTime) + ",定时器时间：" + new Timestamp(onTime));
        //注册定时器
        ctx.timerService().registerEventTimeTimer(onTime);
        //更新状态
        state.update(onTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        //定时器执行逻辑
        //处理完之后，不清空定时器，让定时器能继续执行
        String result = "线程ID:" + Thread.currentThread().getId() + ",key:" + ctx.getCurrentKey().getField(0) + " , 触发时间:" + new Timestamp(timestamp);
        out.collect(result);
        state.clear();
    }
}
