/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.validator;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DBReader {

	protected DstType dbType;
	protected String connStr;
	protected static long RETRY_QUERY_INTERVAL_MS = 5000L;
	protected static long RETRY_QUERY_COUNT = 10;
	protected Logger tracer;
	protected Properties props;

	public DBReader(DstType dstType, String connStr, Properties props, Logger tracer) {
		this.dbType = dstType;
		this.connStr = connStr;
		this.props = props;
		this.tracer = tracer;
	}

	public List<String> readRows(String sql) throws SyncLiteTestException, InterruptedException {
		List<String> result = new ArrayList<String>();
		for (int attempt=1 ; attempt<= RETRY_QUERY_COUNT ; ++attempt) {
			result.clear();
			try (Connection conn = DriverManager.getConnection(connStr, this.props)) {
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt.executeQuery(sql)) {					
						while (rs.next()) {
							StringBuilder rowBuilder = new StringBuilder();
							boolean firstField = true;
							for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
								if (!firstField) {
									rowBuilder.append("|");
								}
								rowBuilder.append(rs.getString(i));
								firstField = false;
							}
							result.add(rowBuilder.toString());
						}
					}
				}
				return result;
			} catch (Exception e) {
				if (attempt < RETRY_QUERY_COUNT) {
					tracer.error("Query Failed on " + dbType + " at URL : " + connStr + " with sql : " + sql + " with error : " + e.getMessage(), e);
					Thread.sleep(RETRY_QUERY_INTERVAL_MS);
				} else { 
					throw new SyncLiteTestException("All retry attemptes failed to read rows from DB : " + dbType + " at URL : " + connStr + " : " + e.getMessage(), e);
				}
			}
		}
		return result;
	}	

	public long readScalarLong(String sql) throws SyncLiteTestException, InterruptedException {
		for (int attempt=1 ; attempt<= RETRY_QUERY_COUNT ; ++attempt) {
			try (Connection conn = DriverManager.getConnection(this.connStr, this.props)) {
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt.executeQuery(sql)) {
						if (rs.next()) {
							return rs.getLong(1);
						}
					}
				}
				return -1;
			} catch (SQLException e) {
				if (attempt < RETRY_QUERY_COUNT) {
					tracer.error("failed to read scalar long from " + dbType + " at URL : " + connStr + " with sql : " + sql + " with error : ", e);
					Thread.sleep(RETRY_QUERY_INTERVAL_MS);
				} else { 
					throw new SyncLiteTestException("All retry attempts failed to read scalar long using sql : " + sql + " from DB : " + dbType + " at URL : " + connStr + " : ", e);
				}				
			}
		}
		return -1;
	}
	
}
