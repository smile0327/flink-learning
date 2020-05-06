package com.bjsxt.flink.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestUnion {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    var stream1 =streamEnv.fromElements(("a",1),("b",2))
    var stream2 =streamEnv.fromElements(("b",5),("d",6))
    var stream3 =streamEnv.fromElements(("e",7),("f",8))

    val result: DataStream[(String, Int)] = stream1.union(stream2,stream3)

    result.print()

    streamEnv.execute()


  }
}
