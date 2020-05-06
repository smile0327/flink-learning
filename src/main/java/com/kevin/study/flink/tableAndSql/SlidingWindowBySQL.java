package com.kevin.study.flink.tableAndSql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.kevin.study.flink.StationLog;

/**
 * @Auther: kevin
 * @Description: 基于flink sql的滑动窗口操作
 *  每隔5秒统计10秒内每个基站通话成功总时长
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 15:12 2020/4/23
 * @ProjectName: Flink-SXT
 */
public class SlidingWindowBySQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);

        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> input = streamEnv.socketTextStream("10.172.246.227", 8888);

        DataStream<StationLog> logStream = input.map(line -> {
            String[] split = line.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
        })
        //引入watermark 延时5秒
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StationLog>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(StationLog stationLog) {
                return stationLog.getCallTime();
            }
        });

/*
        //基于table api操作
        Table table = tableEnv.fromDataStream(logStream, "sid,callOut,callIn,callType,callTime.rowtime,duration");
        Table result = table.filter("callType = 'success'")
                .window(Slide.over("10.second").every("5.second").on("callTime").as("window"))
                .groupBy("window , sid")
                .select("sid , window.start,window.end,duration.sum");
*/

        //基于sql操作  sql操作使用hop表示窗口，注意与table api使用窗口的时间顺序
        tableEnv.registerDataStream("station" , logStream , "sid,callOut,callIn,callType,callTime.rowtime,duration");
        Table result = tableEnv.sqlQuery("select sid,hop_start(callTime,interval '5' second,interval '10' second), "
                + "hop_end(callTime,interval '5' second,interval '10' second), sum(duration) from station "
                + "where callType = 'success' "
                + "group by hop(callTime,interval '5' second,interval '10' second),sid");

        tableEnv.toRetractStream(result , Row.class).filter(t -> t.f0).print();

        streamEnv.execute("SlidingWindowBySQL");

    }

}
