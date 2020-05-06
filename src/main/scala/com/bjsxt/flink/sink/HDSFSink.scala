package com.bjsxt.flink.sink

import com.bjsxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object HDSFSink {

  //需求：把自定义的Source作为数据源，把基站日志数据写入HDFS并且每隔两秒钟生成一个文件
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    //默认一个小时一个目录(分桶)
    //设置一个滚动策略
    val rolling:DefaultRollingPolicy[StationLog,String] = DefaultRollingPolicy.create()
      .withInactivityInterval(2000)//不活动的分桶时间
      .withRolloverInterval(2000) //每隔两秒生成一个文件
      .build() //创建

    //创建HDFS的Sink
    val hdfsSink: StreamingFileSink[StationLog] = StreamingFileSink.forRowFormat[StationLog](
      new Path("hdfs://hadoop101:9000/MySink001/"),
      new SimpleStringEncoder[StationLog]("UTF-8"))
      .withRollingPolicy(rolling)
      .withBucketCheckInterval(1000) //检查间隔时间
      .build()

    stream.addSink(hdfsSink)

    streamEnv.execute()
  }
}
