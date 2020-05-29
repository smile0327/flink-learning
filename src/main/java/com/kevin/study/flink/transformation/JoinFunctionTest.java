package com.kevin.study.flink.transformation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.util.Arrays;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 15:28 2020/5/29
 * @ProjectName: Flink-SXT
 */
public class JoinFunctionTest {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer , String>> s1 = env.fromCollection(Arrays.asList(
                new Tuple2<Integer , String>(1 , "wuhan"),
                new Tuple2<Integer , String>(2 , "beijing"),
                new Tuple2<Integer , String>(3 , "shanghai")
        ));

        DataSet<Tuple3<Integer , Integer , String>> s2 = env.fromCollection(Arrays.asList(
                new Tuple3<Integer , Integer , String>(1 , 2 , "andy"),
                new Tuple3<Integer , Integer , String>(2 , 2 , "amy"),
                new Tuple3<Integer , Integer , String>(3 , 1 , "jack"),
                new Tuple3<Integer , Integer , String>(4 , 3 , "jane")
        ));

        s2.join(s1)
            .where(t -> (Integer)t.getField(1))
            .equalTo(t -> (Integer)t.getField(0))
            .with((t1,t2) -> t1.getField(0) + "," + t1.getField(2) + "," + t2.getField(1))
            .collect().stream().forEach(r -> System.out.println(r));

    }

}
