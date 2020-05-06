package com.kevin.study.flink.time;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import com.kevin.study.flink.StationLog;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;


/**
 * @Auther: kevin
 * @Description:
 * 每隔10秒统计，每个基站中通话时间最长的一次通话发生的时间还有，
 * 主叫号码，被叫号码，通话时长，并且还得告诉我们当前发生的时间范围（10秒）
 *  迟到数据窗口，waterMark延时5秒 ，lateness再延时5秒   可以处理迟到10秒内的数据
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 17:02 2020/4/20
 * @ProjectName: Flink-SXT
 */
public class LatenessDataOnWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置时间语义  用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        //设置周期生成watermark的时间间隔，默认100ms
//        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> input = env.socketTextStream("10.172.246.227", 8888);

        DataStream<StationLog> logStream = input.map(line -> {
            String[] split = line.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
        });
        OutputTag<StationLog> lateTag = new OutputTag<StationLog>("late" , TypeInformation.of(StationLog.class));
        //设置watermark  允许乱序5秒
        SingleOutputStreamOperator<String> result = logStream.assignTimestampsAndWatermarks(new MyWithPeriodicWatermarks())
                .filter(log -> log.getCallType().equals("success"))
                .keyBy("sid")
                .timeWindow(Time.seconds(10))
                //设置允许迟到5秒   如果窗口已经触发了，再迟到5秒内的数据仍然可以触发窗口，超过5秒输出到侧流
                //触发条件：Watermark < end-of-window + allowedLateness
                .allowedLateness(Time.seconds(5))
                //迟到10（乱序5s + 再迟到5s）秒以上的数据输出到侧流
                .sideOutputLateData(lateTag)
                .reduce(new MaxDurationReduce(), new MaxTimeWindowFunction());

        result.print("main:");
        result.getSideOutput(lateTag).print("late:");

        env.execute("LatenessDataOnWindow");

    }

}

/**
 * 增量聚合函数  每来一条数据都触发计算
 */
class MaxDurationReduce implements ReduceFunction<StationLog> {

    @Override
    public StationLog reduce(StationLog stationLog, StationLog t1) throws Exception {
        return stationLog.getDuration() > t1.getDuration() ? stationLog : t1;
    }

}

/**
 * 窗口触发时才执行这个
 */
class MaxTimeWindowFunction implements WindowFunction<StationLog , String , Tuple , TimeWindow>{
    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<StationLog> iterable, Collector<String> collector) throws Exception {
        StationLog value = iterable.iterator().next();
        StringBuilder sb = new StringBuilder();
        Object key = tuple.getField(0);
        sb.append("窗口范围是:").append(timeWindow.getStart()).append("|").append(new Timestamp(timeWindow.getStart()))
                .append("----").append(timeWindow.getEnd()).append("|").append(new Timestamp(timeWindow.getEnd()));
        sb.append("\n");
        sb.append("key:" + key);
        sb.append("呼叫时间：").append(value.getCallTime())
                .append("主叫号码：").append(value.getCallOut())
                .append("被叫号码：").append(value.getCallIn())
                .append("通话时长：").append(value.getDuration());
        collector.collect(sb.toString());
    }
}

/**
 * 自定义watermark
 * 实现AssignerWithPeriodicWatermarks接口，周期性的生成watermark
 * AssignerWithPunctuatedWatermarks : 间断性的生成watermark，针对某些特殊事件生成watermark，其他事件不返回watermark
 */
class MyWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<StationLog>{

    long maxEventTime = 0L;
    //允许乱序时长
    final long maxOutOfOrderness = 5000L;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * 生成watermark
     * 默认100ms生成一次
     * @return
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxEventTime - maxOutOfOrderness);
    }

    /**
     * 提取eventTime  该方法返回EventTime
     * @param stationLog
     * @param l
     * @return
     */
    @Override
    public long extractTimestamp(StationLog stationLog, long l) {
        long eventTime = stationLog.getCallTime();
        maxEventTime = Math.max(maxEventTime , eventTime);
        System.out.println("key:"+stationLog.getSid()+",eventtime:["+eventTime+"|"+sdf.format(eventTime)+"],currentMaxTimestamp:["+maxEventTime+"|"+
                sdf.format(maxEventTime)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]");
        return eventTime;
    }

}
