package com.kevin.study.flink.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * @Auther: kevin
 * @Description:   读取文件创建table
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 15:56 2020/4/22
 * @ProjectName: Flink-SXT
 */
public class CreateTableByFileSource {

    public static void main(String[] args) throws Exception {

        //使用原生planner 创建table environment
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);

        streamEnv.setParallelism(1);

        //定义数据源 并设置schema信息 （字段和类型）
        CsvTableSource tableSource = new CsvTableSource(CreateTableByFileSource.class.getResource("/station.log").getPath(),
                new String[]{"sid", "callOut", "callIn", "callType", "callTime", "duration"},
                new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG});


/*
        //注册为一张表 直接通过sql来操作
        tableEnv.registerTableSource("station" , tableSource);
        //统计每个基站得总时长
        //只能用toRetractStream转换结果，toAppendStream不能做聚合，使用toRetractStream时结果集中会对一个状态，true标识最新的结果
        //false表示删除得中间结果
        Table table = tableEnv.sqlQuery("select sid,sum(duration) from station group by sid");
        //直接查询可以用toAppendStream转换结果，没有聚合操作
        //Table table = tableEnv.sqlQuery("select * from station where callType = 'success'");
*/

        // 使用tableApi操作数据  统计每个基站童话成功的总通话时长
        Table table = tableEnv.fromTableSource(tableSource);
        Table result = table.filter("callType = 'success'")
                .groupBy("sid")
                .select("sid , duration.sum AS ds");

        tableEnv.toRetractStream(result , Row.class)
                .filter(tuple -> tuple.f0)
                .print();

        streamEnv.execute("tableByFile");
    }

}
