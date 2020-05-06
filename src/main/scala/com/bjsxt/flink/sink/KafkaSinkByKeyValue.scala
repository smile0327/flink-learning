package com.bjsxt.flink.sink

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaSinkByKeyValue {

  //Kafka作为Sink的第二种（KV）
  //把netcat作为数据源，统计每个单词的数量，并且把统计的结果写入Kafka
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._;
    streamEnv.setParallelism(1)

    //读取数据源
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888)

    //计算
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //创建连接Kafka的属性
    var props =new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")

    //创建一个Kafka的sink
    var kafkaSink=new FlinkKafkaProducer[(String,Int)](
      "t_2020",
      new KafkaSerializationSchema[(String, Int)] { //自定义的匿名内部类
        override def serialize(element: (String, Int), timestamp: lang.Long) = {
          new ProducerRecord("t_2020",element._1.getBytes,(element._2+"").getBytes)
        }
      },
      props,//连接Kafka的数学
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE //精确一次
    )

    result.addSink(kafkaSink)

    streamEnv.execute("kafka的sink的第二种")
    //--property print.key=true Kafka的命令加一个参数
  }
}
