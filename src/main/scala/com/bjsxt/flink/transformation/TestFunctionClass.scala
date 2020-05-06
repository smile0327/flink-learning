package com.bjsxt.flink.transformation

import java.text.SimpleDateFormat
import java.util.Date

import com.bjsxt.flink.source.StationLog
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestFunctionClass {

  //计算出每个通话成功的日志中呼叫起始和结束时间,并且按照指定的时间格式
  //数据源来自本地文件
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    var filePath =getClass.getResource("/station.log").getPath
    val stream: DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })

    //定义一个时间格式
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //计算通话成功的起始和结束时间
    val result: DataStream[String] = stream.filter(_.callType.equals("success"))
      .map(new MyMapFunction(format))
    result.print()

    streamEnv.execute()

  }

  //自定义一个函数类
  class MyMapFunction(format:SimpleDateFormat) extends MapFunction[StationLog,String]{
    override def map(value: StationLog): String = {
      var startTime=value.callTime;
      var endTime= startTime + value.duration*1000
      "主叫号码："+value.callOut+",被叫号码:"+value.callInt+",呼叫起始时间:"+format.format(new Date(startTime))+",呼叫结束时间:"+format.format(new Date(endTime))
    }
  }
}
