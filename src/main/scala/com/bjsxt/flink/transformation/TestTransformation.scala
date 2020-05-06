package com.bjsxt.flink.transformation

import com.bjsxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestTransformation {

  //需求：从自定义的数据源中读取基站通话日志，统计每个基站通话成功的总时长是多少秒。
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    //计算
    val result: DataStream[(String, Long)] = stream.filter(_.callType.equals("success")) //过滤通话成功的日志
      .map(log => {
        (log.sid, log.duration)
      }) //转换为二元组
      .keyBy(0)
      .reduce((t1, t2) => {
        var duration = t1._2 + t2._2
        (t1._1, duration)
      })

    result.print()

    streamEnv.execute()

  }
}
