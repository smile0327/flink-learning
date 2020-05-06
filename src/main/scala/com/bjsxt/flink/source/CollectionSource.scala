package com.bjsxt.flink.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 基站日志
 *
 * @param sid 基站的id
 * @param callOut 主叫号码
 * @param callInt 被叫号码
 * @param callType 呼叫类型
 * @param callTime 呼叫时间 (毫秒)
 * @param duration 通话时长 （秒）
 */
case class StationLog(sid:String,var callOut:String,var callInt:String,callType:String,callTime:Long,duration:Long)

object CollectionSource {
  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1
    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[StationLog] = streamEnv.fromCollection(Array(
      new StationLog("001", "1866", "189", "busy", System.currentTimeMillis(), 0),
      new StationLog("002", "1866", "188", "busy", System.currentTimeMillis(), 0),
      new StationLog("004", "1876", "183", "busy", System.currentTimeMillis(), 0),
      new StationLog("005", "1856", "186", "success", System.currentTimeMillis(), 20)
    ))


    stream.print()

    streamEnv.execute()
  }

}
