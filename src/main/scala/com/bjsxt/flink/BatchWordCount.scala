package com.bjsxt.flink

import java.net.URL

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * Flink的批计算案例
 */
object BatchWordCount {

  def main(args: Array[String]): Unit = {
    //初始化Flink批处理环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataPath: URL = getClass.getResource("/wc.txt") //使用相对路径来得到完整的文件路径

    //读数据
    val data: DataSet[String] = env.readTextFile(dataPath.getPath) //DataSet ==> spark RDD

    //计算并且打印结果
    data.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
