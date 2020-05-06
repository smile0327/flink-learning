package com.kevin.study.flink.tableAndSql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.kevin.study.flink.StationLog;

/**
 * @Auther: kevin
 * @Description:  基于flink sql的滚动窗口操作
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 13:38 2020/4/23
 * @ProjectName: Flink-SXT
 */
public class TumbleWindowBySQL {

    public static void main(String[] args) throws Exception {

        //1、创建table执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);

        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //输入数据源
        DataStreamSource<String> input = streamEnv.socketTextStream("10.172.246.227", 8888);
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
        //1、注册为table 需要指定EventTime字段 使用sql
        tableEnv.registerDataStream("station" , logStream , "sid,callOut,callIn,callType,callTime.rowtime,duration");
        //每10秒统计每个基站通话成功总时长  使用sql统计
        Table result = tableEnv.sqlQuery("select sid,sum(duration) as sd "
            + "from station "
            + "where callType='success' "
            + "group by tumble(callTime,interval '10' second),sid");
*/


        //2、每10秒统计每个基站通话成功总时长  使用table api统计
        Table table = tableEnv.fromDataStream(logStream, "sid,callOut,callIn,callType,callTime.rowtime,duration");
        Table result = table.filter("callType='success'")
                .window(Tumble.over("10.second").on("callTime").as("window"))
                //先按照窗口分组，再按照sid分组
                .groupBy("window,sid")
                .select("sid , window.start,window.end,duration.sum");
        tableEnv.toRetractStream(result , Row.class).filter(t -> t.f0).print();

        streamEnv.execute("TumbleWindowBySQL");

    }

}
