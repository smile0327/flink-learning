package com.kevin.study.flink.agg;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;


/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 16:23 2020/5/18
 * @ProjectName: Flink-SXT
 */
public class AggregationsTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3> data = Arrays.asList(
            new Tuple3(0,1,0),
            new Tuple3(0,1,1),
            new Tuple3(0,2,2),
            new Tuple3(0,1,3),
            new Tuple3(1,1,2),
            new Tuple3(1,2,4),
            new Tuple3(1,1,6),
            new Tuple3(1,2,5),
            new Tuple3(2,1,0),
            new Tuple3(2,1,3)
        );

        DataStreamSource<Tuple3> input = streamEnv.fromCollection(data);

        //max()只会返回指定字段的最大值，其他信息不会保存
//        input.keyBy(0).max(2).printToErr();
        //maxBy()会将最大的那条记录的所有信息都返回
        input.keyBy(0).maxBy(2).printToErr();

        streamEnv.execute("aggTest");

    }

}
