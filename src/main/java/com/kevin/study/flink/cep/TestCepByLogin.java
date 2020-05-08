package com.kevin.study.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;


import com.kevin.study.flink.LoginEvent;
import com.kevin.study.flink.StationLog;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Auther: kevin
 * @Description: cep测试
 * 从一堆的登录日志中，匹配一个恶意登录的模式（如果一个用户连续（在10秒内）失败三次，则是恶意登录），
 * 从而找到哪些用户名是恶意登录
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 11:41 2020/5/7
 * @ProjectName: Flink-SXT
 */
public class TestCepByLogin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义事件时间
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(1);

        DataStream<LoginEvent> input = streamEnv.fromCollection(Arrays.asList(
                new LoginEvent(1, "张三", "fail", 1577080457L),
                new LoginEvent(2, "张三", "fail", 1577080458L),
//                new LoginEvent(7, "张三", "logging", 1577080459L),
                new LoginEvent(3, "张三", "fail", 1577080460L),
                new LoginEvent(4, "李四", "fail", 1577080458L),
                new LoginEvent(5, "李四", "success", 1577080462L),
                new LoginEvent(6, "张三", "fail", 1577080462L)
        ))
          //注册watermark  乱序时间为0
          .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<LoginEvent>() {

            long maxEventTime = 0L;
            //允许乱序时长
            final long maxOutOfOrderness = 2000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxEventTime);
            }

            @Override
            public long extractTimestamp(LoginEvent loginEvent, long l) {
                return loginEvent.getEventTime() * 1000;
            }
        });

        //1、定义规则
        //匹配一个恶意登录的模式（如果一个用户连续（在10秒内）失败三次，则是恶意登录）
        //每个规则都是以begin开始   把每个规则直接定义出来
        /*
            模式序列
            1、严格邻近 next：所有事件都按照顺序满足模式条件，不允许忽略任意不满足的模式。
            2、宽松邻近 followedBy：会忽略没有 成功匹配的模式条件（匹配成功后直接往后推）
            3、非确定宽松邻近 followedByAny：和宽松邻近条件相比，非确定宽松邻近条件指在 模式匹配过程中可以忽略已经匹配的条件
         */
        Pattern<LoginEvent, LoginEvent> nextPattern = Pattern.<LoginEvent>begin("start")
                //第一个fail
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                }).next("fail2").where(new SimpleCondition<LoginEvent>() {
                    //第二个fail
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                }).followedBy("fail3").where(new SimpleCondition<LoginEvent>() {
                    //第三个fail
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                })
                //时间限制  10秒内进行匹配，超过这个范围则失效
                .within(Time.seconds(10));

        //2、模式检测   需要按照用户分组
        PatternStream<LoginEvent> patternStream = CEP.pattern(input.keyBy("userName"), nextPattern);

        /*
            3、选择结果
            3.1  select function 抽取正常事件
            3.2  flat select function 抽取正常事件，可以返回任意数量的结果
            3.3  process function
         */
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            /**
             * map中的key为模式序列中pattern的名称，value为对应的pattern所接受的事件集合
             *
             * @param map
             * @return
             * @throws Exception
             */
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                StringBuffer sb = new StringBuffer();
                String userName = null;
                for (Map.Entry<String, List<LoginEvent>> entry : map.entrySet()) {
                    String patternName = entry.getKey();
                    List<LoginEvent> patternValue = entry.getValue();
                    System.out.println(patternName + ":" + patternValue.toString());
                    if (userName == null) {
                        userName = patternValue.get(0).getUserName();
                    }
                    sb.append(patternValue.get(0).getEventTime()).append(",");
                }
                return userName + " -> " + sb.toString();
            }
        });

        //打印匹配结果
        result.print("result:");

        streamEnv.execute("cepTest");

    }




}
