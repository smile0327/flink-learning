package com.kevin.study.flink.tableAndSql;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.kevin.study.flink.StationLog;

/**
 * @Auther: kevin
 * @Description:  从DataStream转换为Table
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 17:13 2020/4/22
 * @ProjectName: Flink-SXT
 */
public class CreateTableByDataStream {

    public static void main(String[] args) throws Exception {
        //使用原生planner 创建table environment   如果使用blinkPlanner需要引入blink的依赖
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);

        streamEnv.setParallelism(1);

        DataStream<StationLog> input = streamEnv.socketTextStream("10.172.246.227", 8888)
                .map(line -> {
                    String[] split = line.trim().split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });

        //注册为表,并更改字段名  原生sql查询
//        tableEnv.registerDataStream("station" , input , "sid as c1,callOut as c2,callIn as c3,callType as c4,callTime as c5,duration as c6");
//        Table result = tableEnv.sqlQuery("select c1 , count(c1) from station where c4 = 'success' group by c1");

        //默认会使用原来的字段名作为列名
        Table table = tableEnv.fromDataStream(input, "sid as c1,callOut as c2,callIn as c3,callType as c4,callTime as c5,duration as c6" );
        table.printSchema();
        Table result = table.filter("c4 = 'success'")
                .groupBy("c1")
                .select("c1 , count(c1)");

        tableEnv.toRetractStream(result , Row.class)
                .filter(tuple -> tuple.f0)
                .print();

        streamEnv.execute("tableByDS");

    }

}
