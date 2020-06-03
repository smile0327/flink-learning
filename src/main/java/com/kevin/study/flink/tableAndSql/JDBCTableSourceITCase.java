/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kevin.study.flink.tableAndSql;

import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * ITCase for {@link JDBCTableSource}.
 */
public class JDBCTableSourceITCase {

	public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
	public static final String DB_URL = "jdbc:mysql://10.172.246.234:3306/ems_jingmen?autoReconnect=true&zeroDateTimeBehavior=convertToNull&useSSL=false";
	public static final String CONNECT_USERNAME = "root";
	public static final String CONNECT_PASSWORD = "bobandata123";
	public static final String INPUT_TABLE_BASEVOLTAGE = "basevoltage";
	public static final String INPUT_TABLE_VOLTAGELEVEL = "voltagelevel";

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
				.useOldPlanner()
				.inStreamingMode()
				.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		String bvStmt = "CREATE TABLE " + INPUT_TABLE_BASEVOLTAGE + "(" +
				"id BIGINT," +
				"system_id VARCHAR," +
				"mrid VARCHAR," +
				"name VARCHAR," +
				"nomkv VARCHAR," +
				"last_refresh_time TIMESTAMP(3)" +
				") WITH (" +
				"  'connector.type' = 'jdbc'," +
				"  'connector.url' = '" + DB_URL + "'," +
				"  'connector.table' = '" + INPUT_TABLE_BASEVOLTAGE + "'," +
				"	'connector.driver' = '" + DRIVER_CLASS + "'," +
				"	'connector.username' = '" + CONNECT_USERNAME + "'," +
				"	'connector.password' = '" + CONNECT_PASSWORD + "'" +
				")";
		tEnv.sqlUpdate(
				bvStmt
		);
		System.out.println(bvStmt + ";");

		String vlStmt = "CREATE TABLE " + INPUT_TABLE_VOLTAGELEVEL + "(" +
				"id BIGINT," +
				"system_id VARCHAR," +
				"mrid VARCHAR," +
				"name VARCHAR," +
				"substation VARCHAR," +
				"baseVoltage VARCHAR," +
				"last_refresh_time TIMESTAMP(3)" +
				") WITH (" +
				"  'connector.type' = 'jdbc'," +
				"  'connector.url' = '" + DB_URL + "'," +
				"  'connector.table' = '" + INPUT_TABLE_VOLTAGELEVEL + "'," +
				"	'connector.driver' = '" + DRIVER_CLASS + "'," +
				"	'connector.username' = '" + CONNECT_USERNAME + "'," +
				"	'connector.password' = '" + CONNECT_PASSWORD + "'" +
				")";

		tEnv.sqlUpdate(
				vlStmt
		);

		System.out.println(vlStmt + ";");

/*
		DataStream<Row> bvStream = tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE_BASEVOLTAGE), Row.class)
				.filter(tuple -> tuple.f0)
				.map(tuple -> tuple.f1);

		DataStream<Row> vlStream = tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE_VOLTAGELEVEL), Row.class)
				.filter(tuple -> tuple.f0)
				.map(tuple -> tuple.f1);

		bvStream.join(vlStream)
				.where(row -> row.getField(2).toString())
				.equalTo(row -> row.getField(5).toString())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.apply(
						new JoinFunction<Row, Row, Tuple3<String,String,Integer>>() {
							@Override
							public Tuple3<String, String, Integer> join(Row r1, Row r2) throws Exception {
								return new Tuple3<>(r1.getField(2).toString(),r2.getField(4).toString(),1);
							}
						}
				).print();
*/

		Table subCntTable = tEnv.sqlQuery("SELECT vl.substation,count(*) FROM " + INPUT_TABLE_VOLTAGELEVEL + " AS vl GROUP BY vl.substation");
		tEnv.toRetractStream(subCntTable , Row.class)
				.filter(t->t.f0)
				.print();

		Table queryTable = tEnv.sqlQuery("SELECT vl.system_id,vl.mrid,vl.name,vl.substation,vl.baseVoltage,bv.name,bv.nomkv " +
				" FROM " + INPUT_TABLE_BASEVOLTAGE + " AS bv JOIN " + INPUT_TABLE_VOLTAGELEVEL + " AS vl " +
				" ON bv.mrid = vl.baseVoltage");

//		tEnv.toRetractStream(queryTable , Row.class).print();

		env.execute();

	}

}
