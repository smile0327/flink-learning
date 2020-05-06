package com.bjsxt.flink.transformation

import com.bjsxt.flink.source.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object TestProcessFunction {

  //监控每一个手机号码，如果这个号码在5秒内，所有呼叫它的日志都是失败的，则发出告警信息
  //如果在5秒内只要有一个呼叫不是fail则不用告警
  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val input = streamEnv.socketTextStream("10.172.246.231", 8888)
    //读取数据源
    val stream: DataStream[StationLog] = input
      .map(line => {
        var arr = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //计算
    val result: DataStream[String] = stream.keyBy(_.callInt)
      .process(new MonitorCallFail)
    result.print()

    streamEnv.execute()

  }

  //自定义一个底层的类
  class MonitorCallFail extends KeyedProcessFunction[String, StationLog, String] {
    //使用一个状态对象记录时间
    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

    override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
      //从状态中取得时间
      var time = timeState.value()
      if (time == 0 && value.callType.equals("fail")) { //表示第一次发现呼叫失败，记录当前的时间
        //获取当前系统时间，并注册定时器
        var nowTime = ctx.timerService().currentProcessingTime()
        //定时器在5秒后触发
        var onTime = nowTime + 5 * 1000L
        ctx.timerService().registerProcessingTimeTimer(onTime)
        //把触发时间保存到状态中
        timeState.update(onTime)
      }
      if (time != 0 && !value.callType.equals("fail")) { //表示有一次成功的呼叫,必须要删除定时器
        ctx.timerService().deleteProcessingTimeTimer(time)
        timeState.clear() //清空状态中的时间
      }
    }

    //时间到了，定时器执行,
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
      var warnStr = "触发的时间：" + timestamp + " 手机号 ：" + ctx.getCurrentKey
      out.collect(warnStr)
      timeState.clear()
    }
  }

}
