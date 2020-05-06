package com.kevin.study.flink.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.kevin.study.flink.udf.MySplitFunc;

import java.util.Arrays;

import scala.Tuple2;

/**
 * @Auther: kevin
 * @Description:  使用Table API通过自定义函数实现wordcount
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 10:16 2020/4/23
 * @ProjectName: Flink-SXT
 */
public class WordCountByUDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);

        streamEnv.setParallelism(1);

        DataStream<String> input = streamEnv.socketTextStream("10.172.246.227", 8888);

        //转换为Table  并将字段名设置为line
        Table table = tableEnv.fromDataStream(input, "line");

        //注册一个function
        tableEnv.registerFunction("split" , new MySplitFunc());
        Table result = table.flatMap("split(line)").as("word , word_c")
                .groupBy("word")
                .select("word , word_c.sum as cnt");

        tableEnv.toRetractStream(result , Row.class).filter(t -> t.f0).print();

        streamEnv.execute("WordCountByUDF");

    }


}
