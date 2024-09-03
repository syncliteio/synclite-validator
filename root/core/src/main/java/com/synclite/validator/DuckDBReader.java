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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class DuckDBReader extends DBReader {

	public DuckDBReader(DstType dstType, String connStr, Properties props, Logger tracer) {
		super(dstType, connStr, props, tracer);
	}

	@Override
	public List<String> readRows(String sql) throws SyncLiteTestException, InterruptedException {
		List<String> result = new ArrayList<String>();
		for (int attempt=1; attempt<=RETRY_QUERY_COUNT; ++attempt) {
			result.clear();
			try (ZContext context = new ZContext()) {								
				//  Socket to talk to server
				ZMQ.Socket socket = context.createSocket(SocketType.REQ);
				socket.connect("tcp://localhost:9999");
				String req = "GetTextOutput " + sql;
				socket.send(req.getBytes(ZMQ.CHARSET), 0);
				byte[] reply = socket.recv(0);
				String output = new String(reply, ZMQ.CHARSET);

				String[] outputRows = output.split("\n");
				//Ignore first header row 

				for (int i=1 ; i <outputRows.length ; ++i) {
					result.add(outputRows[i]);             
				}
				return result;
			} catch (Exception e) {
				if (attempt < RETRY_QUERY_COUNT) {
					tracer.error("Failed to read rows from DB : " + dbType + " at URL : " + connStr + " with sql : " + sql + " with error :", e);
					Thread.sleep(RETRY_QUERY_INTERVAL_MS);
				} else {
					throw new SyncLiteTestException("All retry attempts failed to read rows from DB : " + dbType + " at URL : " + connStr + " : ", e);
				}
			}
		}
		return result;
	}	

	@Override
	public long readScalarLong(String sql) throws SyncLiteTestException, InterruptedException {
		for (int attempt=1; attempt<=RETRY_QUERY_COUNT; ++attempt) {

			try (ZContext context = new ZContext()) {								
				//  Socket to talk to server
				ZMQ.Socket socket = context.createSocket(SocketType.REQ);
				socket.connect("tcp://localhost:9999");
				String req = "GetTextOutput " + sql;
				socket.send(req.getBytes(ZMQ.CHARSET), 0);
				byte[] reply = socket.recv(0);
				String output = new String(reply, ZMQ.CHARSET);

				String[] outputRows = output.split("\n");

				if (outputRows.length >=2) {
					try {
						return Long.valueOf(outputRows[1]);
					} catch (NumberFormatException e) {
						throw new SyncLiteTestException("Failed to read scalar long using sql : " + sql + " from DB : " + dbType + " at URL : " + connStr + ", received output : " + output);
					}
				}
				return -1;
			} catch (Exception e) {
				if (attempt < RETRY_QUERY_COUNT) {
					tracer.error("Failed to read scalar long from DB : " + dbType + " at URL : " + connStr + " wth sql : " + sql , e);
					Thread.sleep(RETRY_QUERY_INTERVAL_MS);
				} else {
					throw new SyncLiteTestException("All retry attempts failed to read scalar long from DB : " + dbType + " at URL : " + connStr + " : ", e);
				}

			}
		}
		return -1;
	}

}
