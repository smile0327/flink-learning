package com.kevin.study.flink.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;


/**
 * @Auther: kevin
 * @Description:  kafka connector 从kafka读取数据写入mysql
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 15:33 2020/4/24
 * @ProjectName: Flink-SXT
 */
public class SQLSubmitByKafkaSourceByConnect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv , setting);
        streamEnv.setParallelism(1);

        //1、使用API的形式从kafka获取数据
        tableEnv.connect(
                //指定kafka connector
                new Kafka()
                        .version("universal")
                        .topic("VL_JM_EMS_CIME_TOPIC")
                        .property("zookeeper.connect", "10.172.246.231:2181,10.172.246.232:2181,10.172.246.233:2181")
                        .property("bootstrap.servers", "10.172.246.231:9092,10.172.246.232:9092,10.172.246.233:9092")
                        .property("group.id", "testGroup")
                        .startFromEarliest()
        )
        .withFormat(
                new Json()
                    .failOnMissingField(true)
                    //从指定得schema中逆推
                    .deriveSchema()
        )
        //指定table schema信息
        .withSchema(
                new Schema()
                .field("pathName" , Types.STRING)
                .field("Substation" , Types.STRING)
                .field("mRID" , Types.STRING)
                .field("BaseVoltage" , Types.STRING)
                .field("system_id" , Types.STRING)
                .field("Num" , Types.STRING)
                .field("name" , Types.STRING)
                .field("lowkV" , Types.STRING)
                .field("type" , Types.STRING)
                .field("highkV" , Types.STRING)
        )
        //数据更新模式
        .inAppendMode()
        //注册为table
        .registerTableSource("vl_source");

        Table result = tableEnv.sqlQuery("select * from vl_source");
        tableEnv.toRetractStream(result , Row.class).print();

        String insertSql = "INSERT INTO vl_sink (pathName,Substation,mRID,BaseVoltage,system_id,Num,name,lowkV,type,highkV) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?)";

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDBUrl("jdbc:mysql://10.172.246.234:3306/ems_jingmen")
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("bobandata123")
                .setBatchSize(10)
                .setQuery(insertSql)
                .setParameterTypes(
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO,
                        STRING_TYPE_INFO
                )
                .build();

        tableEnv.registerTableSink(
                "t_vl_sink" ,
                //定义schema信息
                new String[]{"pathName" , "Substation" , "mRID" , "BaseVoltage" , "system_id" , "Num" , "name" , "lowkV" , "type" , "highkV"} ,
                new TypeInformation[]{Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING},
                sink);

        result.insertInto("t_vl_sink");

        streamEnv.execute("SQLSubmitByKafkaSourceByConnect");

    }

}
