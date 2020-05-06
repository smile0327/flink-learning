package com.bjsxt.flink.tableAndSql

import com.bjsxt.flink.source.StationLog
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
 * 统计每个基站中，通话成功的通话总时长
 */
object TestSQLByDurationCount {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //两个隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)


//    //读取数据
//    val tableSource = new CsvTableSource(getClass.getResource("/station.log").getPath,
//      Array[String]("sid", "call_out", "call_in", "call_type", "call_time", "duration"),
//      Array(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG)
//    )

    //使用纯粹的SQL
    //注册表
//    tableEnv.registerTableSource("t_station_log",tableSource)
//    //执行sql
//    val result: Table = tableEnv.sqlQuery("select sid,sum(duration) as d_c " +
//      "from t_station_log where call_type='success' group by sid")

    //TableAPI和sql混用
    val stream: DataStream[StationLog] = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
        .map(line=>{
          val arr: Array[String] = line.split(",")
          new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
        })

    val table: Table = tableEnv.fromDataStream(stream)
//    tableEnv.registerTable("station" , table)
    //执行sql  s开头代表字符串替换，替换的字符串以$开头；也可以注册为一张表，然后查询表中的数据
    val result: Table = tableEnv.sqlQuery(s"select  sid,sum(duration) as d_c from $table where callType='success' group by sid")


    //打印结果
    tableEnv.toRetractStream[Row](result)
      .filter(_._1==true)
      .print()
    tableEnv.execute("sql")

  }
}
