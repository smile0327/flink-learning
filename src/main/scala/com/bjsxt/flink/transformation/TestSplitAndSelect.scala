package com.bjsxt.flink.transformation

import com.bjsxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestSplitAndSelect {

  //需求：从自定义的数据源中读取基站通话日志，把通话成功的和通话失败的分离出来
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    //切割
    val splitStream: SplitStream[StationLog] = stream.split( //流并没有真正切割
      log => {
        if (log.callType.equals("success")) {
          Seq("Success")
        }else{
          Seq("NO Success")
        }
      }
    )
    //选择不同的流
    var stream1 =splitStream.select("Success") //根据标签得到不同流
    var stream2 =splitStream.select("No Success")

    stream.print("原始数据")
    stream1.print("通话成功")
//    stream2.print("通话不成功")

    streamEnv.execute()

  }
}
