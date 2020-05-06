package com.bjsxt.flink.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSinkByString {

  //Kafka作为Sink的第一种（String）
  //需求：把netcat数据源中每个单词写入Kafka
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)

    //读取数据源
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888)

    //计算
    val words: DataStream[String] = stream.flatMap(_.split(" "))

    //把单词写入Kafka
    words.addSink(new FlinkKafkaProducer[String]("hadoop101:9092,hadoop102:9092,hadoop103:9092","t_2020",new SimpleStringSchema()))

    streamEnv.execute("kafkaSink")
  }
}
