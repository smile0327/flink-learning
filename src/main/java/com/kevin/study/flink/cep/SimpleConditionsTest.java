package com.kevin.study.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


import com.kevin.study.flink.Event;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Auther: kevin
 * @Description: 简单条件测试  仅使用where/or/量词 过滤
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 15:00 2020/5/8
 * @ProjectName: Flink-SXT
 */
public class SimpleConditionsTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        //输入数据源
        DataStream<Event> input = streamEnv.fromCollection(Arrays.asList(
                new Event(1L, "a1", "add", 1588298400L),
                new Event(2L, "a2", "add", 1588298402L),
                new Event(3L, "a12", "add", 1588298400L),
                new Event(4L, "b11", "add", 1588298412L),
                new Event(5L, "b12", "add", 1588298400L),
                new Event(6L, "c3", "add", 1588298400L),
                new Event(7L, "b13", "add", 1588298400L)
        ));

        //1、定义规则  匹配以a和c开头的用户
        Pattern<Event, Event> pattern = getGreedyPattern();

        //2、模式检测
        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        patternStream.select(
                new PatternSelectFunction<Event, String>() {
                    //返回匹配数据的id
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        StringBuffer sb = new StringBuffer();
                        for (Map.Entry<String, List<Event>> entry : map.entrySet()) {
                            Iterator<Event> iterator = entry.getValue().iterator();
                            iterator.forEachRemaining(i -> sb.append(i.getName()).append(","));
                            sb.append("|").append(",");
                        }
                        sb.delete(sb.length() - 3 , sb.length());
//                        sb.deleteCharAt(sb.length() - 1);
                        return sb.toString();
                    }
                }
        ).print();

        streamEnv.execute("simpleCEPTest");

    }

    private static Pattern<Event, Event> getNotFollowedBy() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }
        ).times(1, 2).notFollowedBy("notFollowed").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("b");
                    }
                }
        ).next("next").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("c");
                    }
                }
        );
    }

    private static Pattern<Event, Event> getNotNextPattern() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }
        ).times(1, 2).notNext("notNext").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("c");
                    }
                }
        );
    }

    private static Pattern<Event, Event> getUntilPattern() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }
        ).oneOrMore().until(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("b");
                    }
                }
        );
    }

    private static Pattern<Event, Event> getFollowedByAnyPattern() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }
        ).times(1, 2).followedByAny("followedByAny").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("c");
                    }
                }
        ).times(1, 2);
    }

    private static Pattern<Event, Event> getFollowedByPattern() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }).times(1, 2).followedBy("followedBy").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("c");
                    }
                }
        ).times(1, 2);
    }

    private static Pattern<Event, Event> getNextPattern() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }).times(1, 2).next("middle").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("c");
                    }
                }
        ).times(1, 2);
    }

    private static Pattern<Event, Event> getGreedyPattern() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }
        ).times(2, 3).greedy().next("middle").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().length() == 3;
                    }
                }).times(1, 2);
    }

    private static Pattern<Event, Event> getWhereOrPattern() {
        return Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }
        ).or(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("c");
                    }
                }
        );
    }

}
