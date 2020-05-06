package com.bjsxt.flink.transformation

import com.bjsxt.flink.source.StationLog
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object TestSideOutputStream {
  import org.apache.flink.streaming.api.scala._
  var notSuccessTag =new OutputTag[StationLog]("not_success")//不成功的侧流标签
  //把呼叫成功的日志输出到主流，不成功的到侧流
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //读取数据源
    var filePath =getClass.getResource("/station.log").getPath
    print(filePath)
    val stream: DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })

    //
    val result: DataStream[StationLog] = stream.process(new CreateSideOuputStream(notSuccessTag))

    result.print("主流")
    //一定要根据主流得到则流
    val sideStream: DataStream[StationLog] = result.getSideOutput(notSuccessTag)
    sideStream.print("侧流")

    streamEnv.execute()
  }

  class  CreateSideOuputStream(tag:OutputTag[StationLog]) extends ProcessFunction[StationLog,StationLog]{
    override def processElement(value: StationLog, ctx: ProcessFunction[StationLog, StationLog]#Context, out: Collector[StationLog]): Unit = {
      if(value.callType.equals("success")){
        out.collect(value) //输出主流
      }else{ //输出侧流
        ctx.output(tag,value)
      }
    }
  }
}
