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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.synclite.logger.*;

public class TestDriver implements Runnable{

	private Path testRoot;
	private Path dbDir;
	private Path stageDir;
	private Path commandDir;
	private Path workDir;
	private Path loggerConfig;
	private Path consolidatorConfig;
	private Path corePath;
	private PreparedStatement insertPstmtValidatorDB;
	private PreparedStatement updatePstmtValidatorDB;
	private Connection validatorDBConn;
	private long lastTestStartTime;
	private long lastTestFinishTime;
	private Logger globalTracer;
	private HashMap<String, String> consolidatorConfigs = new HashMap<String, String>();
	private DBReader dstDBReader;
	private String dstTablePrefix;
	private String mode;
	private static final String DEVICE_COMMIT_ID_READER_QUERY = "SELECT MAX(commit_id) FROM synclite_txn";
	private static final Long CONSOLIDATION_WAIT_DURATION_MS = 300000L;
	private static final Long CONSOLIDATION_CHECK_INTERVAL = 5000L;
	private static final Long CONSOLIDATOR_JOB_WAIT_DURATION_MS = 300000L;

	public TestDriver(Path testRoot, Path loggerConfig, Path consolidatorConfig, Path corePath) throws SyncLiteTestException {
		this.testRoot = testRoot;
		this.loggerConfig = loggerConfig;		
		this.consolidatorConfig = consolidatorConfig;
		this.corePath = corePath;
		this.dbDir = testRoot.resolve("db");
		this.stageDir = testRoot.resolve("stageDir");
		this.workDir = testRoot.resolve("workDir");
		this.commandDir = testRoot.resolve("commandDir");
		try {
			initTracer();

			validatorDBConn =  DriverManager.getConnection("jdbc:sqlite:" + this.workDir.resolve("synclite_validator.db"));
			try (Statement stmt = validatorDBConn.createStatement()) {
				stmt.execute("CREATE TABLE IF NOT EXISTS test_results(test_name TEXT PRIMARY KEY, start_time TEXT, end_time TEXT, execution_time LONG, status TEXT)");
			}
			insertPstmtValidatorDB = validatorDBConn.prepareStatement("INSERT INTO test_results(test_name, start_time, end_time, execution_time, status) VALUES(?, ?, ?, ?, ?)");
			updatePstmtValidatorDB = validatorDBConn.prepareStatement("UPDATE test_results SET end_time = ?, execution_time = ?, status = ? WHERE test_name = ?");

			Class.forName("io.synclite.logger.SQLite");
			Class.forName("io.synclite.logger.SQLiteAppender");
			Class.forName("io.synclite.logger.DuckDB");
			Class.forName("io.synclite.logger.DuckDBAppender");
			Class.forName("io.synclite.logger.Derby");
			Class.forName("io.synclite.logger.DerbyAppender");
			Class.forName("io.synclite.logger.H2");
			Class.forName("io.synclite.logger.H2Appender");
			Class.forName("io.synclite.logger.HyperSQL");
			Class.forName("io.synclite.logger.HyperSQLAppender");
			Class.forName("io.synclite.logger.Streaming");
			Class.forName("io.synclite.logger.Telemetry");

			loadConsolidatorConfig();

			//TODO Generalize for multiple destinations.
			initDstDBReader(1);

		} catch (SQLException | ClassNotFoundException e) {
			this.globalTracer.error("Failed to initialize TestDriver", e);
			throw new SyncLiteTestException("Failed to initialize TestDriver", e);
		}
	}

	private final void stopConsolidatorJob() throws SyncLiteTestException {
		globalTracer.info("Stopping consolidator job");
		try {
			long currentJobPID = getConsolidatorJobPID();
			if(currentJobPID > 0) {
				if (isWindows()) {
					Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
				} else {
					Runtime.getRuntime().exec("kill -9 " + currentJobPID);
				}
			}
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to stop consolidator job : ", e);
		}
	}

	private final long getConsolidatorJobPID() throws SyncLiteTestException {
		try {
			//Get current job PID if running
			long currentJobPID = 0;
			Process jpsProc;
			if (isWindows()) {
				String javaHome = System.getenv("JAVA_HOME");			
				String scriptPath = "jps";
				if (javaHome != null) {
					scriptPath = javaHome + "\\bin\\jps";
				} else {
					scriptPath = "jps";
				}
				String[] cmdArray = {scriptPath, "-l", "-m"};
				jpsProc = Runtime.getRuntime().exec(cmdArray);
			} else {
				String javaHome = System.getenv("JAVA_HOME");			
				String scriptPath = "jps";
				if (javaHome != null) {
					scriptPath = javaHome + "/bin/jps";
				} else {
					scriptPath = "jps";
				}
				String[] cmdArray = {scriptPath, "-l", "-m"};
				jpsProc = Runtime.getRuntime().exec(cmdArray);
			}

			BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
			String line = stdout.readLine();
			while (line != null) {
				if (line.contains("com.synclite.consolidator.Main")) {
					currentJobPID = Long.valueOf(line.split(" ")[0]);
				}
				line = stdout.readLine();
			}
			return currentJobPID;
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to stop consolidator job : ", e);
		}
	}

	private final void startSyncConsolidatorJob() throws SyncLiteTestException {
		globalTracer.info("Starting sync consolidator job");
		try {
			String corePath = this.corePath.toString();
			String deviceDataRoot = this.workDir.toString();
			String propsPath = Path.of(this.workDir.toString(), "synclite_consolidator.conf").toString();

			Process p;
			if (isWindows()) {
				String scriptName = "synclite-consolidator.bat";
				String scriptPath = Path.of(corePath, scriptName).toString();
				String[] cmdArray = {scriptPath, "sync", "--work-dir", deviceDataRoot, "--config", propsPath};
				p = Runtime.getRuntime().exec(cmdArray);						
				
			} else {
				String scriptName = "synclite-consolidator.sh";
				Path scriptPath = Path.of(corePath, scriptName);
				
				// Get the current set of script permissions
				Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
				// Add the execute permission if it is not already set
				if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
					perms.add(PosixFilePermission.OWNER_EXECUTE);
					Files.setPosixFilePermissions(scriptPath, perms);
				}

				String[] cmdArray = {scriptPath.toString(), "sync", "--work-dir", deviceDataRoot, "--config", propsPath};
				p = Runtime.getRuntime().exec(cmdArray);					
			}

		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to start sync consolidator job : ", e);
		}
	}

	private final void startManageDevicesConsolidatorJob() throws SyncLiteTestException {
		globalTracer.info("Starting manage devices consolidator job");
		try {
			String corePath = this.corePath.toString();
			String deviceDataRoot = this.workDir.toString();
			String propsPath = Path.of(this.workDir.toString(), "synclite_consolidator.conf").toString();
			String manageDevicesPropsPath = Path.of(this.workDir.toString(), "synclite_consolidator_manage_devices.conf").toString();

			Process p;
			if (isWindows()) {
				String scriptName = "synclite-consolidator.bat";
				String scriptPath = Path.of(corePath, scriptName).toString();
				String[] cmdArray = {scriptPath, "manage-devices", "--work-dir", deviceDataRoot, "--config", propsPath, "--manage-devices-config", manageDevicesPropsPath};
				p = Runtime.getRuntime().exec(cmdArray);						
				
			} else {
				String scriptName = "synclite-consolidator.sh";
				Path scriptPath = Path.of(corePath, scriptName);
				
				String[] cmdArray = {scriptPath.toString(), "manage-devices", "--work-dir", deviceDataRoot, "--config", propsPath, "--manage-devices-config", manageDevicesPropsPath};
				p = Runtime.getRuntime().exec(cmdArray);					
			}

		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to start manage-devices consolidator job : ", e);
		}
	}
	
	private final boolean isWindows() {
		String osName = System.getProperty("os.name").toLowerCase();
		if (osName.contains("win")) {
			return true;
		}
		return false;
	}

	private void initDstDBReader(int dstIndex) {
		DstType dstType = DstType.valueOf(consolidatorConfigs.get("dst-type-" + dstIndex));		
		String dstConnStr = consolidatorConfigs.get("dst-connection-string-" + dstIndex);

		if (dstConnStr == null) {
			if (dstType == DstType.SQLITE) {
				dstConnStr = "jdbc:sqlite:" + this.workDir.resolve("consolidated_db_" + dstIndex + ".sqlite");
			} else if(dstType == DstType.DUCKDB) {
				dstConnStr = "jdbc:duckdb:" + this.workDir.resolve("consolidated_db_" + dstIndex + ".duckdb");
			}
		}

		Properties props = new Properties();
		if (dstType == DstType.DUCKDB) {
			//We cannot connect to DuckDB using JDBC in this process to read data since it is being written
			//onto by consolidator process. Hence we send sql to the DuckDB listener running inside consolidator process.
			props.setProperty("duckdb.read_only", "true");
			this.dstDBReader = new DuckDBReader(dstType, dstConnStr, props, this.globalTracer);
		} else {
			this.dstDBReader = new DBReader(dstType, dstConnStr, props, this.globalTracer);
		}

		String dstDatabase = consolidatorConfigs.get("dst-database-" + dstIndex);
		String dstSchema = consolidatorConfigs.get("dst-schema-" + dstIndex);
		this.mode = consolidatorConfigs.get("dst-sync-mode");

		if (mode.equals("CONSOLIDATION")) {
			if (dstDatabase == null) {
				if (dstSchema == null) {
					this.dstTablePrefix = "";
				} else {
					this.dstTablePrefix = dstSchema + ".";
				}
			} else {
				if (dstSchema == null) {
					this.dstTablePrefix = dstDatabase + ".";
				} else {
					this.dstTablePrefix = dstDatabase + "." + dstSchema + ".";
				}			
			}		
		} else {
			//TODO Handle REPLICATION mode
		}
	}

	private final void loadConsolidatorConfig() throws SyncLiteTestException {
		try (BufferedReader reader = new BufferedReader(new FileReader(consolidatorConfig.toFile()))) {
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=");
				if (tokens.length < 2) {
					continue;
				}
				String propName = tokens[0].trim().toLowerCase();
				String propValue = line.substring(line.indexOf("=") + 1, line.length()).trim();
				consolidatorConfigs.put(propName, propValue);
				line = reader.readLine();
			}
		} catch (IOException e) {
			throw new SyncLiteTestException("Failed to load configurations from consolidation config file : " + consolidatorConfig, e);
		}
	}


	public void runTests() throws SyncLiteTestException {
		createMockDevice();	
		//Add test method calls here

		testSQLiteStmtBasic();
		testSQLitePreparedStmtBasic();
		testSQLiteTableMerge();
		testSQLiteCommitRollback();
		testSQLiteFatTableAutoArgInlining();
		testSQLiteFatTableFixedInlinedArgs();

		testDuckDBStmtBasic();
		testDuckDBPreparedStmtBasic();
		testDuckDBCommitRollback();

		testDerbyStmtBasic();
		testDerbyPreparedStmtBasic();
		testDerbyCommitRollback();

		testH2StmtBasic();
		testH2PreparedStmtBasic();
		testH2CommitRollback();

		testHyperSQLStmtBasic();
		testHyperSQLPreparedStmtBasic();
		testHyperSQLCommitRollback();

		testTelemetryPreparedStmtBasic();
		testTelemetryFatTableAutoArgInlining();
		testTelemetryFatTableFixedInlinedArgs();
		testTelemetryInsertWithColList();

		testStreamingPreparedStmtBasic();
	
		testSQLiteAppenderPreparedStmtBasic();
		testDuckDBAppenderPreparedStmtBasic();
		testDerbyAppenderPreparedStmtBasic();
		testH2AppenderPreparedStmtBasic();
		testHyperSQLAppenderPreparedStmtBasic();

		testSQLiteAppenderFatTableAutoArgInlining();
		testSQLiteAppenderFatTableFixedInlinedArgs();
		testSQLiteAppenderInsertWithColList();

		testSQLiteCallback();
		testSQLiteReinitializeDevice();
	}

	private void createMockDevice() throws SyncLiteTestException {		
		globalTracer.error("Testing mock device");
		String testName = "mockTest";
		try {
			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLite.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using statement
			//3. UPDATE a row using statement
			//4. DELETE a row using statement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE mock(col1 INTEGER)");
					stmt.execute("INSERT INTO mock VALUES(1)");
				}
			}
			waitForConsolidationStartup();

			SQLite.closeDevice(testDBPath);
			globalTracer.error("Verified mock device.");
		} catch (SQLException e) {
			globalTracer.error("Failed to create a mock device woth exception:", e);
			throw new SyncLiteTestException("Failed to create a mock device with exception : " , e);
		}
	}

	private final void initTracer() {
		this.globalTracer = Logger.getLogger(TestDriver.class);    	
		globalTracer.setLevel(Level.INFO);
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("FileLogger");
		fa.setFile(workDir.resolve("synclite_validator.trace").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setAppend(true);
		fa.activateOptions();
		globalTracer.addAppender(fa);
	}

	private final void preTest(String testName) throws SyncLiteTestException {		
		String startTimeStr = Instant.now().toString().replace("T", " ").replace("Z", "");
		lastTestStartTime = System.currentTimeMillis();
		try {
			insertPstmtValidatorDB.clearBatch();
			insertPstmtValidatorDB.setString(1, testName);
			insertPstmtValidatorDB.setString(2, startTimeStr);
			insertPstmtValidatorDB.setString(3, null);
			insertPstmtValidatorDB.setLong(4, 0);
			insertPstmtValidatorDB.setString(5, "RUNNING");
			insertPstmtValidatorDB.execute();
			globalTracer.info("Started Test : " + testName);
		} catch (SQLException e) {
			throw new SyncLiteTestException("Failed to excecute test " + testName + " in preTest phase : ", e);
		}
	}

	private final void postTest(String testName, String executionStatus) throws SyncLiteTestException {		
		String endTimeStr = Instant.now().toString().replace("T", " ").replace("Z", "");		
		lastTestFinishTime = System.currentTimeMillis();
		long testExecutionTime = lastTestFinishTime - lastTestStartTime;
		try {
			updatePstmtValidatorDB.clearBatch();
			updatePstmtValidatorDB.setString(1, endTimeStr);
			updatePstmtValidatorDB.setLong(2, testExecutionTime);
			updatePstmtValidatorDB.setString(3, executionStatus);
			updatePstmtValidatorDB.setString(4, testName);
			updatePstmtValidatorDB.execute();
			globalTracer.info("Finished Test : " + testName);
		} catch (SQLException e) {
			throw new SyncLiteTestException("Failed to excecute test " + testName + " in postTest phase : ", e);
		}
	}

	private final void verifyDataWithDevice(String deviceName, DeviceType deviceType, Path devicePath, String tabName, List<String> cols, List<String> orderCols) throws SyncLiteTestException, InterruptedException {
		globalTracer.info("Verifying data for table " + tabName + " in destination with respect to device :" + deviceName);

		StringBuilder deviceSqlBuilder = new StringBuilder();
		StringBuilder dstSqlBuilder = new StringBuilder();
		deviceSqlBuilder.append("SELECT ");
		dstSqlBuilder.append("SELECT ");
		boolean first = true;
		for (String s : cols) {
			if (first == false) {
				deviceSqlBuilder.append(", ");
				dstSqlBuilder.append(", ");
			}
			deviceSqlBuilder.append(s);
			dstSqlBuilder.append(s);
			first = false;
		}

		StringBuilder orderByClauseBuilder = new StringBuilder();
		if (orderCols.size() > 0) {
			orderByClauseBuilder.append(" ORDER BY ");
			boolean firstCol = true;
			for (String s : orderCols) {
				if (firstCol == false) {
					orderByClauseBuilder.append(",");
				}
				orderByClauseBuilder.append(s);
				firstCol = false;
			}
		}

		deviceSqlBuilder.append(" FROM ");
		deviceSqlBuilder.append(tabName);
		deviceSqlBuilder.append(orderByClauseBuilder.toString());

		dstSqlBuilder.append(" FROM ");
		dstSqlBuilder.append(this.dstTablePrefix + tabName);
		dstSqlBuilder.append(" WHERE synclite_device_name = '" +  deviceName + "'");	
		dstSqlBuilder.append(orderByClauseBuilder.toString());

		DBReader deviceDBReader = null;
		Properties props = new Properties();
		if (deviceType == DeviceType.DUCKDB || deviceType == DeviceType.DUCKDB_APPENDER) {			
			props.setProperty("duckdb.read_only", "true");
			deviceDBReader = new DBReader(DstType.DUCKDB, "jdbc:duckdb:" + devicePath, props, this.globalTracer);
		} else if (deviceType == DeviceType.DERBY || deviceType == DeviceType.DERBY_APPENDER) {
	        props.setProperty("readonly", "true"); // Set read-only property
			deviceDBReader = new DBReader(DstType.DERBY, "jdbc:derby:" + devicePath, props, this.globalTracer);
		} else if (deviceType == DeviceType.H2 || deviceType == DeviceType.H2_APPENDER) {
			deviceDBReader = new DBReader(DstType.H2, "jdbc:h2:" + devicePath, props, this.globalTracer);
		} else if (deviceType == DeviceType.HYPERSQL || deviceType == DeviceType.HYPERSQL_APPENDER) {
			deviceDBReader = new DBReader(DstType.HYPERSQL, "jdbc:hsqldb:" + devicePath, props, this.globalTracer);
		} else {
			deviceDBReader = new DBReader(DstType.SQLITE, "jdbc:sqlite:" + devicePath, props, this.globalTracer);
		}
		
		List<String> deviceDataRows = deviceDBReader.readRows(deviceSqlBuilder.toString());
		List<String> dstDataRows = dstDBReader.readRows(dstSqlBuilder.toString());

		String sql = dstSqlBuilder.toString();
		//Compare deviceDataRows and dstDataRows
		if (deviceDataRows.size() != dstDataRows.size()) {
			dumpRows(deviceDataRows, dstDataRows, sql);
			throw new SyncLiteTestException("Count mismatch identified. Device row count: " + deviceDataRows.size() + " . Destination row count : " + dstDataRows.size());
		}

		for (int i = 0; i < deviceDataRows.size(); i++) {
			String deviceRow = deviceDataRows.get(i);
			String dstRow = dstDataRows.get(i);

			if (! deviceRow.equals(dstRow)) {
				throw new SyncLiteTestException("Data mismatch identified at row number : " + i + ". Device row : " + deviceRow + ". Destination row : " + dstRow);
			}
		}
	}

	private final void verifyData(List<String> expectedRows, String tabName, List<String> cols, List<String> orderCols) throws SyncLiteTestException, InterruptedException {	
		globalTracer.info("Verifying data for table :" + tabName);
		StringBuilder dstSqlBuilder = new StringBuilder();
		dstSqlBuilder.append("SELECT ");
		boolean first = true;
		for (String s : cols) {
			if (first == false) {
				dstSqlBuilder.append(", ");
			}
			dstSqlBuilder.append(s);
			first = false;
		}
		dstSqlBuilder.append(" FROM ");
		dstSqlBuilder.append(this.dstTablePrefix + tabName);

		StringBuilder orderByClauseBuilder = new StringBuilder();
		if (orderCols.size() > 0) {
			orderByClauseBuilder.append(" ORDER BY ");
			boolean firstCol = true;
			for (String s : orderCols) {
				if (firstCol == false) {
					orderByClauseBuilder.append(",");
				}
				orderByClauseBuilder.append(s);
				firstCol = false;
			}
		}
		dstSqlBuilder.append(orderByClauseBuilder.toString());

		String sql = dstSqlBuilder.toString();
		List<String> dstDataRows = dstDBReader.readRows(sql);

		//Compare expectedRows and dstDataRows

		if (expectedRows.size() != dstDataRows.size()) {			
			dumpRows(expectedRows, dstDataRows, sql);
			throw new SyncLiteTestException("Count mismatch identified. Expected row count: " + expectedRows.size() + " . Destination row count : " + dstDataRows.size());
		}

		for (int i = 0; i < expectedRows.size(); i++) {
			String expectedRow = expectedRows.get(i);
			String dstRow = dstDataRows.get(i);

			if (! expectedRow.equals(dstRow)) {
				dumpRows(expectedRows, dstDataRows, sql);
				throw new SyncLiteTestException("Data mismatch identified at row number : " + i + ". Expected row : " + expectedRow + ". Destination row : " + dstRow);
			}
		}
	}

	private final void dumpRows(List<String> expectedRows, List<String> currentRows, String sql) {
		globalTracer.info("Result verification dump for SQL : " + sql);
		globalTracer.info("Expected Rows : ");
		for (String s : expectedRows) {
			globalTracer.info(s);
		}
		globalTracer.info("Current Rows : ");
		for (String s : currentRows) {
			globalTracer.info(s);
		}		
	}

	private final void waitForConsolidationStartup() throws SyncLiteTestException {
		globalTracer.info("Waiting for data consolidation startup");
		//Read every 5 seconds for 5 minutes and then give up.
		try {
			long waited = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				try {
					String dstQuery = "SELECT commit_id FROM " + dstTablePrefix + "synclite_metadata";
					dstDBReader.readScalarLong(dstQuery);
					globalTracer.info("Verified Data Consolidation startup");
					return;
				} catch (SyncLiteTestException e){
					Thread.sleep(CONSOLIDATION_CHECK_INTERVAL);
					waited += CONSOLIDATION_CHECK_INTERVAL;
				}
			}
			throw new SyncLiteTestException("Data Consolidation has not started within " + CONSOLIDATION_WAIT_DURATION_MS + " (ms).");			
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	private final void waitForConsolidation(String deviceName, DeviceType deviceType, Path devicePath) throws SyncLiteTestException {		
		globalTracer.info("Waiting for data consolidation of the executed workload");
		//Read every 5 seconds for 5 minutes and then give up.
		try {
			long waited = 0;
			long deviceCommitID =0;
			long dstCommitID = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				DBReader deviceDBReader;
				Properties props = new Properties();
				if (deviceType == DeviceType.DUCKDB || deviceType == DeviceType.DUCKDB_APPENDER) {
					props.setProperty("duckdb.read_only", "true");
					deviceDBReader = new DBReader(DstType.DUCKDB, "jdbc:duckdb:" + devicePath.toString(), props, this.globalTracer);
				} else if (deviceType == DeviceType.DERBY || deviceType == DeviceType.DERBY_APPENDER) {
			        props.setProperty("readonly", "true"); // Set read-only property
					deviceDBReader = new DBReader(DstType.DERBY, "jdbc:derby:" + devicePath.toString(), props, this.globalTracer);
				} else if (deviceType == DeviceType.H2 || deviceType == DeviceType.H2_APPENDER) {
					deviceDBReader = new DBReader(DstType.H2, "jdbc:h2:" + devicePath.toString(), props, this.globalTracer);
				} else if (deviceType == DeviceType.HYPERSQL || deviceType == DeviceType.HYPERSQL_APPENDER) {
					deviceDBReader = new DBReader(DstType.HYPERSQL, "jdbc:hsqldb:" + devicePath.toString(), props, this.globalTracer);
				} else {
					deviceDBReader = new DBReader(DstType.SQLITE, "jdbc:sqlite:" + devicePath.toString(), props, this.globalTracer);
				}
				deviceCommitID = deviceDBReader.readScalarLong(DEVICE_COMMIT_ID_READER_QUERY);
				String dstCommitIDQuery = "SELECT commit_id FROM " + this.dstTablePrefix + "synclite_metadata WHERE synclite_device_name = '" + deviceName + "'";

				dstCommitID = dstDBReader.readScalarLong(dstCommitIDQuery);

				if (deviceCommitID == dstCommitID) {
					return;
				} else {
					Thread.sleep(CONSOLIDATION_CHECK_INTERVAL);
					waited += CONSOLIDATION_CHECK_INTERVAL;
				}
			}
			throw new SyncLiteTestException("Data consolidation did not finish in " + CONSOLIDATION_WAIT_DURATION_MS + " (ms). Last read CommitID from device : " + deviceCommitID + ". Last read CommitID from destination : " + dstCommitID);			
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	//=================================================
	//
	//WRITE YOUR TESTS BELOW =========================>
	//
	//=================================================

	private final void testSQLiteStmtBasic() throws SyncLiteTestException {		
		String testName = "testSQLiteStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLite.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using statement
			//3. UPDATE a row using statement
			//4. DELETE a row using statement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
					stmt.execute("INSERT INTO tab1 VALUES(1, 1.1, '1', '1', '1')");
					stmt.execute("INSERT INTO tab1 VALUES(2, 2.2, '2', '2', '2')");
					stmt.execute("INSERT INTO tab1 VALUES(4, 4.4, '4', '4', '4')");
					stmt.execute("INSERT INTO tab1 VALUES(5, 5.5, '5', '5', '5')");

					stmt.execute("UPDATE tab1 SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3', col5 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM tab1 WHERE col1 = 5");
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");
			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");
			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, "tab1", cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testSQLitePreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testSQLitePreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLite.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.setBytes(5, "1".getBytes());
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.setBytes(5, "2".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");;
					pstmt.setBytes(5, "4".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setDouble(2, 5.5);
					pstmt.setString(3, "5");
					pstmt.setString(4, "5");
					pstmt.setBytes(5, "5".getBytes());			
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE tab1 SET col1 = ?, col2 = ?, col3 = ?, col4 = ?, col5 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setBytes(5, "3".getBytes());
					pstmt.setInt(6, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM tab1 WHERE col1 = ?")) {
					pstmt.setInt(1, 5);					
					pstmt.execute();
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, "tab1", cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteTableMerge() throws SyncLiteTestException {		
		String testName = "testSQLiteTableMerge";
		try {
			preTest(testName);

			String deviceName1 = testName + "1";
			Path devicePath1 = dbDir.resolve(deviceName1);
			SQLite.initialize(devicePath1, loggerConfig, deviceName1);
			String deviceURL1 = "jdbc:synclite_sqlite:" + dbDir.resolve(devicePath1);

			String deviceName2 = testName + "2";
			Path devicePath2 = dbDir.resolve(deviceName2);
			SQLite.initialize(devicePath2, loggerConfig, deviceName2);
			String deviceURL2 = "jdbc:synclite_sqlite:" + dbDir.resolve(devicePath2);


			//Test a table merge scenario
			//create two tables on two devices and check of they are merged appropriately
			//		

			try (Connection conn = DriverManager.getConnection(deviceURL1)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + testName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE)");
					stmt.execute("INSERT INTO " + testName + " VALUES(1, 1.1)");
					stmt.execute("INSERT INTO "+ testName +  " VALUES(2, 2.2)");
				}
			}

			try (Connection conn = DriverManager.getConnection(deviceURL2)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + testName + "(col1 INTEGER PRIMARY KEY, col3 TEXT, col4 BLOB)");
					stmt.execute("INSERT INTO " + testName + " VALUES(3, '3', '3')");
					stmt.execute("INSERT INTO "+ testName +  " VALUES(4, '4', '4')");
				}
			}

			waitForConsolidation(deviceName1, DeviceType.SQLITE, devicePath1);

			waitForConsolidation(deviceName2, DeviceType.SQLITE, devicePath2);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(deviceName1, DeviceType.SQLITE, devicePath1, testName, cols, orderCols);

			cols.clear();
			cols.add("col3");
			cols.add("col4");
			verifyDataWithDevice(deviceName2, DeviceType.SQLITE, devicePath2, testName, cols, orderCols);

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|1.1|null|null");
			expectedRows.add("2|2.2|null|null");
			expectedRows.add("3|null|3|3");
			expectedRows.add("4|null|4|4");

			cols.clear();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");			
			verifyData(expectedRows, testName, cols, orderCols);

			SQLite.closeDevice(devicePath1);
			SQLite.closeDevice(devicePath2);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteCommitRollback() throws SyncLiteTestException {		
		String testName = "testSQLiteCommitRollback";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLite.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);


			String tabName1 = testName + "_1";

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName1 + "(col1 INTEGER PRIMARY KEY, col2 TEXT)");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(1, '1')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(2, '2')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(3, '3')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(4, '4')");
				}
				conn.commit();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(5, '5')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(6, '6')");
				}
				conn.rollback();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(7, '7')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(8, '8')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(9, '9')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(10, '10')");
					stmt.execute("commit");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(11, '11')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(12, '12')");
					stmt.execute("rollback");
				}
			}


			String tabName2 = testName + "_2"; 
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(13, '13')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(14, '14')");					
				}

				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName2 + "(col1 INTEGER PRIMARY KEY, col2 TEXT)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName2 + " VALUES(?, ?)")){
					pstmt.setInt(1, 1);
					pstmt.setString(2, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setString(2, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tabName2 + " SET col1 = ?, col2 = ? WHERE col1 = ?")){
					pstmt.setInt(1, 2);
					pstmt.setString(2, "2");
					pstmt.setInt(3, 3);
					pstmt.addBatch();


					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.setInt(3, 5);
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tabName2 + " WHERE col1 = ?")){
					pstmt.setInt(1, 3);
					pstmt.addBatch();

					pstmt.executeBatch();
				}				
				conn.commit();
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, tabName1, cols, orderCols);
			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, tabName2, cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDuckDBStmtBasic() throws SyncLiteTestException {		
		String testName = "testDuckDBStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			DuckDB.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_duckdb:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using statement
			//3. UPDATE a row using statement
			//4. DELETE a row using statement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 TEXT)");
					stmt.execute("INSERT INTO tab1 VALUES(1, 1.1, '1', '1')");
					stmt.execute("INSERT INTO tab1 VALUES(2, 2.2, '2', '2')");
					stmt.execute("INSERT INTO tab1 VALUES(4, 4.4, '4', '4')");
					stmt.execute("INSERT INTO tab1 VALUES(5, 5.5, '5', '5')");

					stmt.execute("UPDATE tab1 SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM tab1 WHERE col1 = 5");
				}
			}

			waitForConsolidation(testName, DeviceType.DUCKDB, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");
			verifyDataWithDevice(testName, DeviceType.DUCKDB, testDBPath, "tab1", cols, orderCols);

			DuckDB.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " +  e.getMessage() + " : " + e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDuckDBPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testDuckDBPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			DuckDB.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_duckdb:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 TEXT)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");;
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setDouble(2, 5.5);
					pstmt.setString(3, "5");
					pstmt.setString(4, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE tab1 SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM tab1 WHERE col1 = ?")) {
					pstmt.setInt(1, 5);
					
					pstmt.execute();
				}
			}

			waitForConsolidation(testName, DeviceType.DUCKDB, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.DUCKDB, testDBPath, "tab1", cols, orderCols);

			DuckDB.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDuckDBCommitRollback() throws SyncLiteTestException {		
		String testName = "testDuckDBCommitRollback";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			DuckDB.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_duckdb:" + dbDir.resolve(testDBPath);


			String tabName1 = testName + "_1";

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName1 + "(col1 INTEGER PRIMARY KEY, col2 TEXT)");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(1, '1')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(2, '2')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(3, '3')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(4, '4')");
				}
				conn.commit();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(5, '5')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(6, '6')");
				}
				conn.rollback();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(7, '7')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(8, '8')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(9, '9')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(10, '10')");
					stmt.execute("commit");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(11, '11')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(12, '12')");
					stmt.execute("rollback");
				}
			}


			String tabName2 = testName + "_2"; 
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(13, '13')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(14, '14')");					
				}

				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName2 + "(col1 INTEGER PRIMARY KEY, col2 TEXT)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName2 + " VALUES(?, ?)")){
					pstmt.setInt(1, 1);
					pstmt.setString(2, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setString(2, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tabName2 + " SET col1 = ?, col2 = ? WHERE col1 = ?")){
					pstmt.setInt(1, 2);
					pstmt.setString(2, "2");
					pstmt.setInt(3, 3);
					pstmt.addBatch();


					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.setInt(3, 5);
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tabName2 + " WHERE col1 = ?")){
					pstmt.setInt(1, 3);
					pstmt.addBatch();

					pstmt.executeBatch();
				}				
				conn.commit();
			}

			waitForConsolidation(testName, DeviceType.DUCKDB, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.DUCKDB, testDBPath, tabName1, cols, orderCols);
			verifyDataWithDevice(testName, DeviceType.DUCKDB, testDBPath, tabName2, cols, orderCols);

			DuckDB.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	
	private final void testDerbyStmtBasic() throws SyncLiteTestException {		
		String testName = "testDerbyStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Derby.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_derby:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using statement
			//3. UPDATE a row using statement
			//4. DELETE a row using statement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
					stmt.execute("INSERT INTO tab1 VALUES(1, 1.1, '1', '1')");
					stmt.execute("INSERT INTO tab1 VALUES(2, 2.2, '2', '2')");
					stmt.execute("INSERT INTO tab1 VALUES(4, 4.4, '4', '4')");
					stmt.execute("INSERT INTO tab1 VALUES(5, 5.5, '5', '5')");

					stmt.execute("UPDATE tab1 SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM tab1 WHERE col1 = 5");
				}
			}

			waitForConsolidation(testName, DeviceType.DERBY, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");
			verifyDataWithDevice(testName, DeviceType.DERBY, testDBPath, "tab1", cols, orderCols);

			Derby.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " +  e.getMessage() + " : " + e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDerbyPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testDerbyPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Derby.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_derby:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");;
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setDouble(2, 5.5);
					pstmt.setString(3, "5");
					pstmt.setString(4, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE tab1 SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM tab1 WHERE col1 = ?")) {
					pstmt.setInt(1, 5);
					
					pstmt.execute();
				}
			}

			waitForConsolidation(testName, DeviceType.DERBY, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.DERBY, testDBPath, "tab1", cols, orderCols);

			Derby.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDerbyCommitRollback() throws SyncLiteTestException {		
		String testName = "testDerbyCommitRollback";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Derby.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_derby:" + dbDir.resolve(testDBPath);


			String tabName1 = testName + "_1";

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName1 + "(col1 INTEGER PRIMARY KEY, col2 VARCHAR(50))");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(1, '1')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(2, '2')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(3, '3')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(4, '4')");
				}
				conn.commit();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(5, '5')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(6, '6')");
				}
				conn.rollback();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(7, '7')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(8, '8')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(9, '9')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(10, '10')");
					stmt.execute("commit");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(11, '11')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(12, '12')");
					stmt.execute("rollback");
				}
			}


			String tabName2 = testName + "_2"; 
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(13, '13')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(14, '14')");					
				}

				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName2 + "(col1 INTEGER PRIMARY KEY, col2 VARCHAR(50))");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName2 + " VALUES(?, ?)")){
					pstmt.setInt(1, 1);
					pstmt.setString(2, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setString(2, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tabName2 + " SET col1 = ?, col2 = ? WHERE col1 = ?")){
					pstmt.setInt(1, 2);
					pstmt.setString(2, "2");
					pstmt.setInt(3, 3);
					pstmt.addBatch();


					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.setInt(3, 5);
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tabName2 + " WHERE col1 = ?")){
					pstmt.setInt(1, 3);
					pstmt.addBatch();

					pstmt.executeBatch();
				}				
				conn.commit();
			}

			waitForConsolidation(testName, DeviceType.DERBY, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.DERBY, testDBPath, tabName1, cols, orderCols);
			verifyDataWithDevice(testName, DeviceType.DERBY, testDBPath, tabName2, cols, orderCols);

			Derby.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	
	private final void testH2StmtBasic() throws SyncLiteTestException {		
		String testName = "testH2StmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			H2.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_h2:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using statement
			//3. UPDATE a row using statement
			//4. DELETE a row using statement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
					stmt.execute("INSERT INTO tab1 VALUES(1, 1.1, '1', '1')");
					stmt.execute("INSERT INTO tab1 VALUES(2, 2.2, '2', '2')");
					stmt.execute("INSERT INTO tab1 VALUES(4, 4.4, '4', '4')");
					stmt.execute("INSERT INTO tab1 VALUES(5, 5.5, '5', '5')");

					stmt.execute("UPDATE tab1 SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM tab1 WHERE col1 = 5");
				}
			}

			waitForConsolidation(testName, DeviceType.H2, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");
			verifyDataWithDevice(testName, DeviceType.H2, testDBPath, "tab1", cols, orderCols);

			H2.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " +  e.getMessage() + " : " + e);
			postTest(testName, "FAIL");
		}
	}


	private final void testH2PreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testH2PreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			H2.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_h2:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");;
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setDouble(2, 5.5);
					pstmt.setString(3, "5");
					pstmt.setString(4, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE tab1 SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM tab1 WHERE col1 = ?")) {
					pstmt.setInt(1, 5);
					
					pstmt.execute();
				}
			}

			waitForConsolidation(testName, DeviceType.H2, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.H2, testDBPath, "tab1", cols, orderCols);

			H2.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testH2CommitRollback() throws SyncLiteTestException {		
		String testName = "testH2CommitRollback";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			H2.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_h2:" + dbDir.resolve(testDBPath);


			String tabName1 = testName + "_1";

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName1 + "(col1 INTEGER PRIMARY KEY, col2 VARCHAR(50))");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(1, '1')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(2, '2')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(3, '3')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(4, '4')");
				}
				conn.commit();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(5, '5')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(6, '6')");
				}
				conn.rollback();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(7, '7')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(8, '8')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(9, '9')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(10, '10')");
					stmt.execute("commit");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(11, '11')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(12, '12')");
					stmt.execute("rollback");
				}
			}


			String tabName2 = testName + "_2"; 
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(13, '13')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(14, '14')");					
				}

				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName2 + "(col1 INTEGER PRIMARY KEY, col2 VARCHAR(50))");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName2 + " VALUES(?, ?)")){
					pstmt.setInt(1, 1);
					pstmt.setString(2, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setString(2, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tabName2 + " SET col1 = ?, col2 = ? WHERE col1 = ?")){
					pstmt.setInt(1, 2);
					pstmt.setString(2, "2");
					pstmt.setInt(3, 3);
					pstmt.addBatch();


					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.setInt(3, 5);
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tabName2 + " WHERE col1 = ?")){
					pstmt.setInt(1, 3);
					pstmt.addBatch();

					pstmt.executeBatch();
				}				
				conn.commit();
			}

			waitForConsolidation(testName, DeviceType.H2, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.H2, testDBPath, tabName1, cols, orderCols);
			verifyDataWithDevice(testName, DeviceType.H2, testDBPath, tabName2, cols, orderCols);

			H2.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	
	
	private final void testHyperSQLStmtBasic() throws SyncLiteTestException {		
		String testName = "testHyperSQLStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			HyperSQL.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_hsqldb:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using statement
			//3. UPDATE a row using statement
			//4. DELETE a row using statement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tabHSQL(col1 INTEGER PRIMARY KEY, col2 INTEGER, col3 VARCHAR(50), col4 VARCHAR(50))");
					stmt.execute("INSERT INTO tabHSQL VALUES(1, 1, '1', '1')");
					stmt.execute("INSERT INTO tabHSQL VALUES(2, 2, '2', '2')");
					stmt.execute("INSERT INTO tabHSQL VALUES(4, 4, '4', '4')");
					stmt.execute("INSERT INTO tabHSQL VALUES(5, 5, '5', '5')");

					stmt.execute("UPDATE tabHSQL SET col1 = 3, col2 = 3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM tabHSQL WHERE col1 = 5");
				}
			}

			waitForConsolidation(testName, DeviceType.HYPERSQL, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");
			verifyDataWithDevice(testName, DeviceType.HYPERSQL, testDBPath, "tabHSQL", cols, orderCols);

			HyperSQL.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " +  e.getMessage() + " : " + e);
			postTest(testName, "FAIL");
		}
	}


	private final void testHyperSQLPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testHyperSQLPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			HyperSQL.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_hsqldb:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tabHSQL(col1 INTEGER PRIMARY KEY, col2 INTEGER, col3 VARCHAR(50), col4 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tabHSQL VALUES(?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");;
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setDouble(2, 5);
					pstmt.setString(3, "5");
					pstmt.setString(4, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE tabHSQL SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM tabHSQL WHERE col1 = ?")) {
					pstmt.setInt(1, 5);
					
					pstmt.execute();
				}
			}

			waitForConsolidation(testName, DeviceType.HYPERSQL, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.HYPERSQL, testDBPath, "tabHSQL", cols, orderCols);

			HyperSQL.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testHyperSQLCommitRollback() throws SyncLiteTestException {		
		String testName = "testHyperSQLCommitRollback";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			HyperSQL.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_hsqldb:" + dbDir.resolve(testDBPath);


			String tabName1 = testName + "_1";

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName1 + "(col1 INTEGER PRIMARY KEY, col2 VARCHAR(50))");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(1, '1')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(2, '2')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(3, '3')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(4, '4')");
				}
				conn.commit();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(5, '5')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(6, '6')");
				}
				conn.rollback();
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(7, '7')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(8, '8')");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(9, '9')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(10, '10')");
					stmt.execute("commit");
				}
			}

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("begin");
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(11, '11')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(12, '12')");
					stmt.execute("rollback");
				}
			}


			String tabName2 = testName + "_2"; 
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("INSERT INTO " + tabName1 + " VALUES(13, '13')");
					stmt.execute("INSERT INTO "+ tabName1 +  " VALUES(14, '14')");					
				}

				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("CREATE TABLE " + tabName2 + "(col1 INTEGER PRIMARY KEY, col2 VARCHAR(50))");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName2 + " VALUES(?, ?)")){
					pstmt.setInt(1, 1);
					pstmt.setString(2, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 5);
					pstmt.setString(2, "5");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tabName2 + " SET col1 = ?, col2 = ? WHERE col1 = ?")){
					pstmt.setInt(1, 2);
					pstmt.setString(2, "2");
					pstmt.setInt(3, 3);
					pstmt.addBatch();


					pstmt.setInt(1, 3);
					pstmt.setString(2, "3");
					pstmt.setInt(3, 5);
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tabName2 + " WHERE col1 = ?")){
					pstmt.setInt(1, 3);
					pstmt.addBatch();

					pstmt.executeBatch();
				}				
				conn.commit();
			}

			waitForConsolidation(testName, DeviceType.HYPERSQL, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.HYPERSQL, testDBPath, tabName1, cols, orderCols);
			verifyDataWithDevice(testName, DeviceType.HYPERSQL, testDBPath, tabName2, cols, orderCols);

			HyperSQL.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testTelemetryPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testTelemetryPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Telemetry.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_telemetry:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1Tel(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1Tel VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.setBytes(5, "1".getBytes());
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.setBytes(5, "2".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");;
					pstmt.setBytes(5, "3".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");
					pstmt.setBytes(5, "4".getBytes());			
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				//Execute this dummy DDL as it will update the checkpoint table 
				//and make it possible to validate if consolidation has succeeded for telemetry device.
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE dummy(col1 int)");
				}				
			}

			waitForConsolidation(testName, DeviceType.TELEMETRY, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|1.1|1|1|1");
			expectedRows.add("2|2.2|2|2|2");
			expectedRows.add("3|3.3|3|3|3");
			expectedRows.add("4|4.4|4|4|4");

			verifyData(expectedRows, "tab1Tel", cols, orderCols);

			Telemetry.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testStreamingPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testStreamingPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Streaming.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_streaming:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1Stream(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1Stream VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.setBytes(5, "1".getBytes());
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.setBytes(5, "2".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");;
					pstmt.setBytes(5, "3".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");
					pstmt.setBytes(5, "4".getBytes());			
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				//Execute this dummy DDL as it will update the checkpoint table 
				//and make it possible to validate if consolidation has succeeded for telemetry device.
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE dummy(col1 int)");
				}				
			}

			waitForConsolidation(testName, DeviceType.STREAMING, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|1.1|1|1|1");
			expectedRows.add("2|2.2|2|2|2");
			expectedRows.add("3|3.3|3|3|3");
			expectedRows.add("4|4.4|4|4|4");

			verifyData(expectedRows, "tab1Stream", cols, orderCols);

			Streaming.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testSQLiteAppenderPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			SQLiteAppender.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite_appender:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1SQLiteAppender(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1SQLiteAppender VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.setBytes(5, "1".getBytes());
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.setBytes(5, "2".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");;
					pstmt.setBytes(5, "3".getBytes());			
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.setString(4, "4");
					pstmt.setBytes(5, "4".getBytes());			
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE_APPENDER, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.SQLITE_APPENDER, testDBPath,  "tab1SQLiteAppender", cols, orderCols);

			SQLiteAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testDuckDBAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testDuckDBAppenderPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			DuckDBAppender.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_duckdb_appender:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1DuckDBAppender(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1DuckDBAppender VALUES(?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.DUCKDB_APPENDER, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.DUCKDB_APPENDER, testDBPath,  "tab1DuckDBAppender", cols, orderCols);

			DuckDBAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testDerbyAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testDerbyAppenderPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			DerbyAppender.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_derby_appender:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1DerbyAppender(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1DerbyAppender VALUES(?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.DERBY_APPENDER, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.DERBY_APPENDER, testDBPath,  "tab1DerbyAppender", cols, orderCols);

			DerbyAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testH2AppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testH2AppenderPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			H2Appender.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_h2_appender:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1H2Appender(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1H2Appender VALUES(?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4.4);
					pstmt.setString(3, "4");
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.H2_APPENDER, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.H2_APPENDER, testDBPath,  "tab1H2Appender", cols, orderCols);

			H2Appender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testHyperSQLAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testHyperSQLAppenderPreparedStmtBasic";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			HyperSQLAppender.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_hsqldb_appender:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1HyperSQLAppender(col1 INTEGER PRIMARY KEY, col2 INTEGER, col3 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO tab1HyperSQLAppender VALUES(?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1);
					pstmt.setString(3, "1");
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2);
					pstmt.setString(3, "2");
					pstmt.addBatch();

					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3);
					pstmt.setString(3, "3");
					pstmt.addBatch();

					pstmt.setInt(1, 4);
					pstmt.setDouble(2, 4);
					pstmt.setString(3, "4");
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.HYPERSQL_APPENDER, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.HYPERSQL_APPENDER, testDBPath,  "tab1HyperSQLAppender", cols, orderCols);

			HyperSQLAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteAppenderFatTableAutoArgInlining() throws SyncLiteTestException {		
		String testName = "testSQLiteAppenderFatTableAutoArgInlining";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLiteAppender.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite_appender:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			


			StringBuilder createTableSqlBuilder = new StringBuilder();
			StringBuilder insertTableSqlBuilder = new StringBuilder();
			createTableSqlBuilder.append("CREATE TABLE " + testName + "(");
			insertTableSqlBuilder.append("INSERT INTO " + testName + " VALUES(");
			int colCount = 50;
			List<String> cols = new ArrayList<String>();
			for (int i=1; i<=colCount; ++i) {
				cols.add("col" + i);
				if (i == 1) {
					createTableSqlBuilder.append("col" + i + " INTEGER");
					insertTableSqlBuilder.append("?");
				} else {
					createTableSqlBuilder.append(", col" + i + " INTEGER");
					insertTableSqlBuilder.append(",?");
				}
			}
			createTableSqlBuilder.append(")");
			insertTableSqlBuilder.append(")");


			int rowCount = 10;
			try (Connection conn = DriverManager.getConnection(testDBURL)) {

				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + testName + "Tmp" + "(A TEXT)");
				}				
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + testName + "Tmp VALUES(?)")) {
					pstmt.setString(1, "test1");
					pstmt.addBatch();
					pstmt.setString(1, "test2");
					pstmt.addBatch();
					pstmt.executeBatch();
				}

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSqlBuilder.toString());
				}
				try (PreparedStatement pstmt = conn.prepareStatement(insertTableSqlBuilder.toString())) {
					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();


					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE_APPENDER, testDBPath);

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.SQLITE_APPENDER,  testDBPath,  testName, cols, orderCols);

			SQLiteAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteAppenderFatTableFixedInlinedArgs() throws SyncLiteTestException {		
		String testName = "testSQLiteAppenderFatTableFixedInlinedArgs";

		try {
			preTest(testName);

			SyncLiteOptions options = SyncLiteOptions.loadFromFile(loggerConfig);
			options.setLogMaxInlineArgs(50);
			options.setDeviceName(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLiteAppender.initialize(testDBPath, options);
			String testDBURL = "jdbc:synclite_sqlite_appender:" + dbDir.resolve(testDBPath);

			StringBuilder createTableSqlBuilder = new StringBuilder();
			StringBuilder insertTableSqlBuilder = new StringBuilder();
			createTableSqlBuilder.append("CREATE TABLE " + testName + "(");
			insertTableSqlBuilder.append("INSERT INTO " + testName + " VALUES(");
			int colCount = 50;
			List<String> cols = new ArrayList<String>();
			for (int i=1; i<=colCount; ++i) {
				cols.add("col" + i);
				if (i == 1) {
					createTableSqlBuilder.append("col" + i + " INTEGER");
					insertTableSqlBuilder.append("?");
				} else {
					createTableSqlBuilder.append(", col" + i + " INTEGER");
					insertTableSqlBuilder.append(",?");
				}
			}
			createTableSqlBuilder.append(")");
			insertTableSqlBuilder.append(")");


			int rowCount = 10;
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSqlBuilder.toString());
				}
				try (PreparedStatement pstmt = conn.prepareStatement(insertTableSqlBuilder.toString())) {
					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();


					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE_APPENDER, testDBPath);

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.SQLITE_APPENDER, testDBPath,  testName, cols, orderCols);

			SQLiteAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testTelemetryFatTableAutoArgInlining() throws SyncLiteTestException {
		String testName = "testTelemetryFatTableAutoArgInlining";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Telemetry.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_telemetry:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			


			StringBuilder createTableSqlBuilder = new StringBuilder();
			StringBuilder insertTableSqlBuilder = new StringBuilder();
			createTableSqlBuilder.append("CREATE TABLE " + testName + "(");
			insertTableSqlBuilder.append("INSERT INTO " + testName + " VALUES(");
			int colCount = 50;
			List<String> cols = new ArrayList<String>();
			for (int i=1; i<=colCount; ++i) {
				cols.add("col" + i);
				if (i == 1) {
					createTableSqlBuilder.append("col" + i + " INTEGER");
					insertTableSqlBuilder.append("?");
				} else {
					createTableSqlBuilder.append(", col" + i + " INTEGER");
					insertTableSqlBuilder.append(",?");
				}
			}
			createTableSqlBuilder.append(")");
			insertTableSqlBuilder.append(")");

			int rowCount = 10;
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + testName + "Tmp" + "(A TEXT)");					
				}				
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + testName + "Tmp VALUES(?)")) {
					pstmt.setString(1, "test1");
					pstmt.addBatch();
					pstmt.setString(1, "test2");
					pstmt.addBatch();
					pstmt.executeBatch();
				}

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSqlBuilder.toString());
				}
				try (PreparedStatement pstmt = conn.prepareStatement(insertTableSqlBuilder.toString())) {
					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();


					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}

				//Execute this dummy DDL as it will update the checkpoint table 
				//and make it possible to validate if consolidation has succeeded for telemetry device.
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE dummy(col1 int)");
				}
			}

			waitForConsolidation(testName, DeviceType.TELEMETRY, testDBPath);

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			List<String> expectedRows = new ArrayList<String>();
			for (int i = 0; i <rowCount; ++i) {
				StringBuilder rowBuilder = new StringBuilder();
				for (int j = 1; j <=colCount; ++j) {
					if (j == 1) {
						rowBuilder.append(j);
					} else {
						rowBuilder.append("|" + j);
					}
				}
				expectedRows.add(rowBuilder.toString());
			}

			for (int i = 0; i <rowCount; ++i) {
				StringBuilder rowBuilder = new StringBuilder();
				for (int j = 1; j <=colCount; ++j) {
					if (j == 1) {
						rowBuilder.append(j);
					} else {
						rowBuilder.append("|" + j);
					}
				}
				expectedRows.add(rowBuilder.toString());
			}

			verifyData(expectedRows, testName, cols, orderCols);

			Telemetry.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testTelemetryFatTableFixedInlinedArgs() throws SyncLiteTestException {
		String testName = "testTelemetryFatTableFixedInlinedArgs";

		try {
			preTest(testName);

			SyncLiteOptions options = SyncLiteOptions.loadFromFile(loggerConfig);
			options.setLogMaxInlineArgs(50);
			options.setDeviceName(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Telemetry.initialize(testDBPath, options);
			String testDBURL = "jdbc:synclite_telemetry:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			


			StringBuilder createTableSqlBuilder = new StringBuilder();
			StringBuilder insertTableSqlBuilder = new StringBuilder();
			createTableSqlBuilder.append("CREATE TABLE " + testName + "(");
			insertTableSqlBuilder.append("INSERT INTO " + testName + " VALUES(");
			int colCount = 50;
			List<String> cols = new ArrayList<String>();
			for (int i=1; i<=colCount; ++i) {
				cols.add("col" + i);
				if (i == 1) {
					createTableSqlBuilder.append("col" + i + " INTEGER");
					insertTableSqlBuilder.append("?");
				} else {
					createTableSqlBuilder.append(", col" + i + " INTEGER");
					insertTableSqlBuilder.append(",?");
				}
			}
			createTableSqlBuilder.append(")");
			insertTableSqlBuilder.append(")");

			int rowCount = 10;
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSqlBuilder.toString());
				}
				try (PreparedStatement pstmt = conn.prepareStatement(insertTableSqlBuilder.toString())) {
					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();


					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();					
				}

				//Execute this dummy DDL as it will update the checkpoint table 
				//and make it possible to validate if consolidation has succeeded for telemetry device.
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE dummy(col1 int)");
				}				

			}

			waitForConsolidation(testName, DeviceType.TELEMETRY, testDBPath);

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			List<String> expectedRows = new ArrayList<String>();
			for (int i = 0; i <rowCount; ++i) {
				StringBuilder rowBuilder = new StringBuilder();
				for (int j = 1; j <=colCount; ++j) {
					if (j == 1) {
						rowBuilder.append(j);
					} else {
						rowBuilder.append("|" + j);
					}
				}
				expectedRows.add(rowBuilder.toString());
			}

			for (int i = 0; i <rowCount; ++i) {
				StringBuilder rowBuilder = new StringBuilder();
				for (int j = 1; j <=colCount; ++j) {
					if (j == 1) {
						rowBuilder.append(j);
					} else {
						rowBuilder.append("|" + j);
					}
				}
				expectedRows.add(rowBuilder.toString());
			}

			verifyData(expectedRows, testName, cols, orderCols);

			Telemetry.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteFatTableAutoArgInlining() throws SyncLiteTestException {		
		String testName = "testSQLiteFatTableAutoArgInlining";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLite.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			


			StringBuilder createTableSqlBuilder = new StringBuilder();
			StringBuilder insertTableSqlBuilder = new StringBuilder();
			createTableSqlBuilder.append("CREATE TABLE " + testName + "(");
			insertTableSqlBuilder.append("INSERT INTO " + testName + " VALUES(");
			int colCount = 50;
			List<String> cols = new ArrayList<String>();
			for (int i=1; i<=colCount; ++i) {
				cols.add("col" + i);
				if (i == 1) {
					createTableSqlBuilder.append("col" + i + " INTEGER");
					insertTableSqlBuilder.append("?");
				} else {
					createTableSqlBuilder.append(", col" + i + " INTEGER");
					insertTableSqlBuilder.append(",?");
				}
			}
			createTableSqlBuilder.append(")");
			insertTableSqlBuilder.append(")");


			int rowCount = 10;
			try (Connection conn = DriverManager.getConnection(testDBURL)) {

				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + testName + "Tmp" + "(A TEXT)");					
				}				
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + testName + "Tmp VALUES(?)")) {
					pstmt.setString(1, "test1");
					pstmt.addBatch();
					pstmt.setString(1, "test2");
					pstmt.addBatch();
					pstmt.executeBatch();
				}

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSqlBuilder.toString());
				}			

				try (PreparedStatement pstmt = conn.prepareStatement(insertTableSqlBuilder.toString())) {
					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();

					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.SQLITE,  testDBPath,  testName, cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void testSQLiteFatTableFixedInlinedArgs() throws SyncLiteTestException {		
		String testName = "testSQLiteFatTableFixedInlinedArgs";

		try {
			preTest(testName);

			SyncLiteOptions options = SyncLiteOptions.loadFromFile(loggerConfig);
			options.setLogMaxInlineArgs(50);
			options.setDeviceName(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			SQLite.initialize(testDBPath, options);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			


			StringBuilder createTableSqlBuilder = new StringBuilder();
			StringBuilder insertTableSqlBuilder = new StringBuilder();
			createTableSqlBuilder.append("CREATE TABLE " + testName + "(");
			insertTableSqlBuilder.append("INSERT INTO " + testName + " VALUES(");
			int colCount = 50;
			List<String> cols = new ArrayList<String>();
			for (int i=1; i<=colCount; ++i) {
				cols.add("col" + i);
				if (i == 1) {
					createTableSqlBuilder.append("col" + i + " INTEGER");
					insertTableSqlBuilder.append("?");
				} else {
					createTableSqlBuilder.append(", col" + i + " INTEGER");
					insertTableSqlBuilder.append(",?");
				}
			}
			createTableSqlBuilder.append(")");
			insertTableSqlBuilder.append(")");


			int rowCount = 10;
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSqlBuilder.toString());
				}
				try (PreparedStatement pstmt = conn.prepareStatement(insertTableSqlBuilder.toString())) {
					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();

					for (int i=0; i < rowCount; ++i) {
						for (int j=1; j <= colCount; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath,  testName, cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	@Override
	public final void run() {
		try {
			Class.forName("io.synclite.logger.SQLite");
			Class.forName("io.synclite.logger.SQLiteAppender");
			Class.forName("io.synclite.logger.DuckDB");
			Class.forName("io.synclite.logger.DuckDBAppender");
			Class.forName("io.synclite.logger.Derby");
			Class.forName("io.synclite.logger.DerbyAppender");
			Class.forName("io.synclite.logger.H2");
			Class.forName("io.synclite.logger.H2Appender");
			Class.forName("io.synclite.logger.HyperSQL");
			Class.forName("io.synclite.logger.HyperSQLAppender");			
			Class.forName("io.synclite.logger.Telemetry");
			Class.forName("io.synclite.logger.Streaming");
			runTests();
		} catch (Exception e) {
			globalTracer.error("ERROR : ", e);
			System.out.println("ERROR : " + e);
			System.exit(1);
		}
	}


	private final void testSQLiteCallback() throws SyncLiteTestException {		
		String testName = "testSQLiteCallback";

		try {
			preTest(testName);

			SyncLiteOptions options = SyncLiteOptions.loadFromFile(loggerConfig);
			options.setDeviceName(testName);
			options.setEnableCommandHandler(true);
			options.setCommandHandlerType(CommandHandlerType.INTERNAL);
			testCallbackCommandHandler cmdHandler = new testCallbackCommandHandler();
			options.setCommandHandlerCallback(cmdHandler);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLite.initialize(testDBPath, options);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

			//Test a basic scenario for device command handler

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
					stmt.execute("INSERT INTO tab1 VALUES(1, 1.1, '1', '1', '1')");
					stmt.execute("INSERT INTO tab1 VALUES(2, 2.2, '2', '2', '2')");
					stmt.execute("INSERT INTO tab1 VALUES(4, 4.4, '4', '4', '4')");
					stmt.execute("INSERT INTO tab1 VALUES(5, 5.5, '5', '5', '5')");
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			String commandToSend = "TEST";
			String commandDetailsToSend = "TEST COMMAND DETAILS";


			String reply = "ERROR: Unknown failure";
			try (ZContext context = new ZContext()) {								
				//  Socket to talk to server
				ZMQ.Socket socket = context.createSocket(SocketType.REQ);
				// Set the ZAP_DOMAIN option to accept requests only from localhost
				socket.setZAPDomain("tcp://localhost");
				int port = 10000;
				socket.connect("tcp://localhost:" + port);
				JSONObject jsonObj = new JSONObject();
				jsonObj.put("type", "COMMAND_DEVICES");
				jsonObj.put("command-devices-name-pattern", testName);
				jsonObj.put("device-command", commandToSend);
				jsonObj.put("device-command-details", commandDetailsToSend);

				socket.send(jsonObj.toString().getBytes(ZMQ.CHARSET), 0);

				reply= new String(socket.recv(0), ZMQ.CHARSET);
				socket.close();						
			}
			if (! reply.startsWith("SUCCESS")) {
				throw new RuntimeException("Failed to send device command, received reply : " + reply);
			}

			Thread.sleep(20000);

			//Validate if command was received by device.

			if (! cmdHandler.receivedCommand.equals(commandToSend)) {
				throw new RuntimeException("Exepcted command : " + commandToSend + ", received command : " + cmdHandler.receivedCommand);
			}

			if (! cmdHandler.receivedCommandDetails.equals(commandDetailsToSend)) {
				throw new RuntimeException("Exepcted command details: " + commandDetailsToSend + ", received command details: " + cmdHandler.receivedCommandDetails);
			}

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	
	private final void testSQLiteReinitializeDevice() throws SyncLiteTestException {		
		String testName = "testSQLiteReinitializeDevice";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLite.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using statement
			//3. UPDATE a row using statement
			//4. DELETE a row using statement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE tab1(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
					stmt.execute("INSERT INTO tab1 VALUES(1, 1.1, '1', '1', '1')");
					stmt.execute("INSERT INTO tab1 VALUES(2, 2.2, '2', '2', '2')");
					stmt.execute("INSERT INTO tab1 VALUES(4, 4.4, '4', '4', '4')");
					stmt.execute("INSERT INTO tab1 VALUES(5, 5.5, '5', '5', '5')");

					stmt.execute("UPDATE tab1 SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3', col5 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM tab1 WHERE col1 = 5");
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);
			
			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");
			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");
			verifyDataWithDevice(testName, DeviceType.SQLITE,testDBPath, "tab1", cols, orderCols);


			//Stop job			
			stopConsolidatorJob();
			
			//Reinitialize the device
			
			createManageDevicesConfigFile(testName);

			startManageDevicesConsolidatorJob();
			
			waitForConsolidatorJobToStop();
			
			startSyncConsolidatorJob();
			
			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("INSERT INTO tab1 VALUES(5, 5.5, '5', '5', '5')");
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, "tab1", cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}


	private final void waitForConsolidatorJobToStop() throws SyncLiteTestException {
		globalTracer.info("Waiting for consolidator job to stop");
		//Read every 5 seconds for 5 minutes and then give up.
		try {
			long waited = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				try {
					long currentJobPID = getConsolidatorJobPID();					
					if (currentJobPID == 0) {
						return;
					}
				} catch (SyncLiteTestException e){
					Thread.sleep(CONSOLIDATION_CHECK_INTERVAL);
					waited += CONSOLIDATION_CHECK_INTERVAL;
				}
			}
			throw new SyncLiteTestException("Data Consolidation has not stopped within " + CONSOLIDATION_WAIT_DURATION_MS + " (ms).");			
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}


	private final void createManageDevicesConfigFile(String deviceNamePattern) throws SyncLiteTestException {
		globalTracer.info("Creating manage-devices job configuration file");
		try {
			StringBuilder builder = new StringBuilder();
			
			builder.append("manage-devices-operation-type = REINITIALIZE_DEVICES");
			builder.append("\n");
			builder.append("manage-devices-name-pattern = " + deviceNamePattern);
			
			Path manageDevicesPropsPath = Path.of(this.workDir.toString(), "synclite_consolidator_manage_devices.conf");
	
			Files.writeString(manageDevicesPropsPath, builder.toString());
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to write manage devices config file ", e);
		}
	}


	public class testCallbackCommandHandler implements SyncLiteCommandHandlerCallback {

		public String receivedCommand = "";
		public String receivedCommandDetails = "";

		@Override
		public void handleCommand(String cmd, Path commandFile) {
			receivedCommand = cmd;
			try {
				receivedCommandDetails = Files.readString(commandFile);
			} catch (IOException e) {
				//Ignore
			}
		}		
	}

	private final void testTelemetryInsertWithColList() throws SyncLiteTestException {		
		String testName = "testTelemetryInsertWithColList";
		String tabName = "testTelemetryInsertWithColList";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			Telemetry.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_telemetry:" + dbDir.resolve(testDBPath);

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + tabName + " (col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName + " VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.setBytes(5, "1".getBytes());
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.setBytes(5, "2".getBytes());			
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName + " (col2, col3, col4, col5, col1) VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setDouble(1, 3.3);
					pstmt.setString(2, "3");
					pstmt.setString(3, "3");;
					pstmt.setBytes(4, "3".getBytes());
					pstmt.setInt(5, 3);
				
					pstmt.addBatch();

					pstmt.setDouble(1, 4.4);
					pstmt.setString(2, "4");
					pstmt.setString(3, "4");
					pstmt.setBytes(4, "4".getBytes());
					pstmt.setInt(5, 4);
					
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				//Execute this dummy DDL as it will update the checkpoint table 
				//and make it possible to validate if consolidation has succeeded for telemetry device.
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE dummy(col1 int)");
				}				
			}

			waitForConsolidation(testName, DeviceType.TELEMETRY, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|1.1|1|1|1");
			expectedRows.add("2|2.2|2|2|2");
			expectedRows.add("3|3.3|3|3|3");
			expectedRows.add("4|4.4|4|4|4");

			verifyData(expectedRows, tabName, cols, orderCols);

			Telemetry.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

	
	private final void testSQLiteAppenderInsertWithColList() throws SyncLiteTestException {		
		String testName = "testSQLiteAppenderInsertWithColList";
		String tabName = "testSQLiteAppenderInsertWithColList";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLiteAppender.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite_appender:" + dbDir.resolve(testDBPath);

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + tabName + " (col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName + " VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setInt(1, 1);
					pstmt.setDouble(2, 1.1);
					pstmt.setString(3, "1");
					pstmt.setString(4, "1");
					pstmt.setBytes(5, "1".getBytes());
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setDouble(2, 2.2);
					pstmt.setString(3, "2");
					pstmt.setString(4, "2");
					pstmt.setBytes(5, "2".getBytes());			
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName + "(col2, col3, col4, col5, col1) VALUES(?, ?, ?, ?, ?)")) {
					pstmt.setDouble(1, 3.3);
					pstmt.setString(2, "3");
					pstmt.setString(3, "3");;
					pstmt.setBytes(4, "3".getBytes());
					pstmt.setInt(5, 3);
				
					pstmt.addBatch();

					pstmt.setDouble(1, 4.4);
					pstmt.setString(2, "4");
					pstmt.setString(3, "4");
					pstmt.setBytes(4, "4".getBytes());
					pstmt.setInt(5, 4);
					
					pstmt.addBatch();

					pstmt.executeBatch();
				}

				//Execute this dummy DDL as it will update the checkpoint table 
				//and make it possible to validate if consolidation has succeeded for telemetry device.
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE dummy(col1 int)");
				}				
			}

			waitForConsolidation(testName, DeviceType.SQLITE_APPENDER, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("col1");
			cols.add("col2");
			cols.add("col3");
			cols.add("col4");
			cols.add("col5");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("col1");

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|1.1|1|1|1");
			expectedRows.add("2|2.2|2|2|2");
			expectedRows.add("3|3.3|3|3|3");
			expectedRows.add("4|4.4|4|4|4");

			verifyData(expectedRows, tabName, cols, orderCols);

			SQLiteAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : ", e);
			postTest(testName, "FAIL");
		}
	}

}
