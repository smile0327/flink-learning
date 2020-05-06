package com.bjsxt.flink.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestConnect {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val stream1: DataStream[(String, Int)] = streamEnv.fromElements(("a",1),("b",2),("c",3))
    val stream2: DataStream[String] = streamEnv.fromElements("e","f","g")

    val stream3: ConnectedStreams[(String, Int), String] = stream1.connect(stream2) //注意得到ConnectedStreams，实际上里面的数据没有真正合并

    //使用CoMap,或者CoFlatmap
    val result: DataStream[(String, Int)] = stream3.map(
      //第一个处理的函数
      t => {
        (t._1, t._2)
      },
      //第二个处理的函数
      t => {
        (t, 0)
      }
    )
    result.print()

    streamEnv.execute()
  }
}
