package com.bjsxt.flink.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object HDFSFileSource {

  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1
    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //读取HDFS文件系统上的文件
    val stream: DataStream[String] = streamEnv.readTextFile("hdfs://hadoop101:9000/wc.txt")

    //单词统计的计算
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //定义sink
    result.print()

    streamEnv.execute("wordcount")

  }
}
