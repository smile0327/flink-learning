package com.bjsxt.flink.transformation

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.bjsxt.flink.source.StationLog
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestRichFunctionClass {

  /**
   * 把通话成功的电话号码转换成真是用户姓名，用户姓名保存在Mysql表中
   * @param args
   */
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

    //计算：把电话号码变成用户姓名
    val result: DataStream[StationLog] = stream.filter(_.callType.equals("success"))
      .map(new MyRichMapFunction)
    result.print()

    streamEnv.execute()
  }

  //自定义一个富函数类
  class MyRichMapFunction extends RichMapFunction[StationLog,StationLog]{
    var conn:Connection=_
    var pst:PreparedStatement=_
    override def open(parameters: Configuration): Unit = {
      conn =DriverManager.getConnection("jdbc:mysql://localhost/test","root","123123")
      pst =conn.prepareStatement("select name from t_phone where phone_number=?")
    }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }

    override def map(value: StationLog): StationLog = {
      println(getRuntimeContext.getTaskNameWithSubtasks)
      //查询主叫号码对应的姓名
      pst.setString(1,value.callOut)
      val result: ResultSet = pst.executeQuery()
      if(result.next()){
        value.callOut=result.getString(1)
      }
      //查询被叫号码对应的姓名
      pst.setString(1,value.callInt)
      val result2: ResultSet = pst.executeQuery()
      if(result2.next()){
        value.callInt=result2.getString(1)
      }
      value
    }
  }
}
