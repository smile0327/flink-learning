package com.kevin.study.flink.tableAndSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 16:37 2020/4/27
 * @ProjectName: Flink-SXT
 */
public class SQLSubmitByKafkaSourceByDDL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv , setting);
        streamEnv.setParallelism(1);

        //2、使用DDL创建kafka source table
        String sourceSql = getKafkaSourceSql();

        tableEnv.sqlUpdate(sourceSql);

//        Table result = tableEnv.sqlQuery("select * from vl_source");
//        tableEnv.toRetractStream(result , Row.class).print();

        //jdbc sink不支持connect，只支持DDL
        String sinkSql = getJdbcSinkSql();

        //创建表
        tableEnv.sqlUpdate(sinkSql);

        //insert 语句不能添加column进去  否则会报错 insert into table (column...) select ...
        String insertSql = getInsertSql();
        //写入数据
        tableEnv.sqlUpdate(insertSql);

        streamEnv.execute("SQLSubmitByKafkaSourceByDDL");

    }

    private static String getInsertSql() {
        return "INSERT INTO vl_sink " +
                " SELECT *,LOCALTIMESTAMP " +
                " from vl_source";
    }

    private static String getKafkaSourceSql(){
        return "CREATE TABLE vl_source (" +
                //schema信息
                "pathName VARCHAR," +
                "Substation VARCHAR," +
                "mRID VARCHAR," +
                "BaseVoltage VARCHAR," +
                "system_id VARCHAR," +
                "Num VARCHAR," +
                "name VARCHAR," +
                "lowkV VARCHAR," +
                "type VARCHAR," +
                "highkV VARCHAR" +
                ") WITH (" +
                "'connector.type' = 'kafka'," +
                "'connector.version' = 'universal'," +
                "'connector.topic' = 'VL_JM_EMS_CIME_TOPIC'," +
                "'connector.properties.0.key' = 'zookeeper.connect'," +
                "'connector.properties.0.value' = '10.172.246.231:2181,10.172.246.232:2181,10.172.246.233:2181'," +
                "'connector.properties.1.key' = 'bootstrap.servers'," +
                "'connector.properties.1.value' = '10.172.246.231:9092,10.172.246.232:9092,10.172.246.233:9092'," +
                "'connector.properties.2.key' = 'group.id'," +
                "'connector.properties.2.value' = 'testGroup'," +
                "'connector.startup-mode' = 'earliest-offset'," +
                //数据更新模式
                "'update-mode' = 'append'," +
                //format 类型
                "'format.type' = 'json'," +
                //从指定得schema中逆推
                "'format.derive-schema' = 'true'" +
                ")";
    }

    private static String getJdbcSinkSql(){
        return "CREATE TABLE vl_sink ( " +
                "pathName VARCHAR," +
                "Substation VARCHAR," +
                "mRID VARCHAR," +
                "BaseVoltage VARCHAR," +
                "system_id VARCHAR," +
                "Num VARCHAR," +
                "name VARCHAR," +
                "lowkV VARCHAR," +
                "type VARCHAR," +
                "highkV VARCHAR" +
                ",last_refresh_time TIMESTAMP" +
                ") WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:mysql://10.172.246.234:3306/ems_jingmen'," +
                "'connector.table' = 'vl_sink'," +
                "'connector.driver' = 'com.mysql.jdbc.Driver'," +
                "'connector.username' = 'root'," +
                "'connector.password' = 'bobandata123'," +
                "'connector.write.flush.max-rows' = '10'" +
                ")";
    }


}
