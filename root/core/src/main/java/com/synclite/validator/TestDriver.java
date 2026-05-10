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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.BrokerConstants;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONArray;
import org.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.synclite.logger.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestDriver implements Runnable{

	private Path testRoot;
	private Path dbDir;
	private Path stageDir;
	private Path commandDir;
	private Path workDir;
	private Path loggerConfig;
	private Path consolidatorConfig;
	private Path corePath;
	private Integer numThreads;
	private PreparedStatement insertPstmtValidatorDB;
	private PreparedStatement updatePstmtValidatorDB;
	private Connection validatorDBConn;
	private ThreadLocal<Long> lastTestStartTime = new ThreadLocal<Long>();
	private ThreadLocal<Long> lastTestFinishTime = new ThreadLocal<Long>();
	private Logger globalTracer;
	private HashMap<String, String> consolidatorConfigs = new HashMap<String, String>();
	private DBReader dstDBReader;
	private String dstTablePrefix;
	private String mode;
	private Path qreaderDbDir;
	private Path qreaderConfigPath;
	private Path qreaderLoggerConf;
	private Path qreaderDeviceDbPath;
	private boolean qreaderEnabled = false;
	private Path dbreaderDbDir;
	private Path dbreaderSrcDbPath;
	private Path dbreaderConfigPath;
	private boolean dbreaderEnabled = false;
	private Server embeddedMqttBroker;
	private static final String QREADER_DEVICE_NAME = "testqreaderdevice";
	private static final String QREADER_TABLE = "testQReader_tbl";
	private static final String MQTT_BROKER_URL = "tcp://localhost:1883";
	private static final String DEVICE_COMMIT_ID_READER_QUERY = "SELECT MAX(commit_id) FROM synclite_txn";
	private static final Long CONSOLIDATION_WAIT_DURATION_MS = 600000L;
	private static final Long CONSOLIDATION_CHECK_INTERVAL = 5000L;
	private static final Long CONSOLIDATOR_JOB_WAIT_DURATION_MS = 300000L;

	public TestDriver(Path testRoot, Path loggerConfig, Path consolidatorConfig, Path corePath, Integer nThr) throws SyncLiteTestException {
		this.testRoot = testRoot;
		this.loggerConfig = loggerConfig;		
		this.consolidatorConfig = consolidatorConfig;
		this.corePath = corePath;
		this.dbDir = testRoot.resolve("db");
		this.stageDir = testRoot.resolve("stageDir");
		this.workDir = testRoot.resolve("workDir");
		this.commandDir = testRoot.resolve("commandDir");
		this.numThreads = nThr;
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
			Class.forName("io.synclite.logger.SQLiteStore");

			loadConsolidatorConfig();

			//TODO Generalize for multiple destinations.
			initDstDBReader(1);

		} catch (SQLException | ClassNotFoundException e) {
			this.globalTracer.error("Failed to initialize TestDriver", e);
			throw new SyncLiteTestException("Failed to initialize TestDriver", e);
		}
	}

	private final void stopConsolidatorJob() throws SyncLiteTestException {
		globalTracer.debug("Stopping consolidator job");
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
		globalTracer.debug("Starting sync consolidator job");
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
		globalTracer.debug("Starting manage devices consolidator job");
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

	private final long getDBReaderJobPID() throws SyncLiteTestException {
		try {
			long currentJobPID = 0;
			String javaHome = System.getenv("JAVA_HOME");
			String jpsExe = (javaHome != null)
					? javaHome + (isWindows() ? "\\bin\\jps" : "/bin/jps")
					: "jps";
			String[] cmdArray = {jpsExe, "-l", "-m"};
			Process jpsProc = Runtime.getRuntime().exec(cmdArray);
			BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
			String line = stdout.readLine();
			while (line != null) {
				if (line.contains("com.synclite.dbreader.Main")) {
					try {
						currentJobPID = Long.valueOf(line.split(" ")[0]);
					} catch (NumberFormatException ignored) {}
				}
				line = stdout.readLine();
			}
			return currentJobPID;
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to get dbreader job PID : ", e);
		}
	}

	private final void startDBReaderJob(Path dbDir, Path config) throws SyncLiteTestException {
		globalTracer.debug("Starting dbreader job");
		try {
			String corePathStr = this.corePath.toString();
			if (isWindows()) {
				String scriptPath = Path.of(corePathStr, "synclite-dbreader.bat").toString();
				String[] cmdArray = {scriptPath, "read", "--db-dir", dbDir.toString(), "--config", config.toString()};
				Runtime.getRuntime().exec(cmdArray);
			} else {
				Path scriptPath = Path.of(corePathStr, "synclite-dbreader.sh");
				Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
				if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
					perms.add(PosixFilePermission.OWNER_EXECUTE);
					Files.setPosixFilePermissions(scriptPath, perms);
				}
				String[] cmdArray = {scriptPath.toString(), "read", "--db-dir", dbDir.toString(), "--config", config.toString()};
				Runtime.getRuntime().exec(cmdArray);
			}
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to start dbreader job : ", e);
		}
	}

	private final void stopDBReaderJob() throws SyncLiteTestException {
		globalTracer.debug("Stopping dbreader job");
		try {
			long currentJobPID = getDBReaderJobPID();
			if (currentJobPID > 0) {
				if (isWindows()) {
					String[] cmdArray = {"taskkill", "/F", "/PID", String.valueOf(currentJobPID)};
					Runtime.getRuntime().exec(cmdArray);
				} else {
					String[] cmdArray = {"kill", "-9", String.valueOf(currentJobPID)};
					Runtime.getRuntime().exec(cmdArray);
				}
			}
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to stop dbreader job : ", e);
		}
	}

	private final void setupAndStartDBReaderJob() throws SyncLiteTestException {
		Path dbreaderScript = this.corePath.resolve(isWindows() ? "synclite-dbreader.bat" : "synclite-dbreader.sh");
		if (!Files.exists(dbreaderScript)) {
			globalTracer.debug("Skipping dbreader setup: dbreader script not found at " + dbreaderScript);
			return;
		}

		String dbReaderTable = "dbreadertable";
		Path dbreaderTestRoot   = this.dbDir.resolve("testdbreader");
		this.dbreaderDbDir      = dbreaderTestRoot.resolve("db");
		Path dbreaderSrcDbDir   = dbreaderTestRoot.resolve("srcDb");
		this.dbreaderSrcDbPath  = dbreaderSrcDbDir.resolve("source.db");
		this.dbreaderConfigPath = dbreaderDbDir.resolve("synclite_dbreader.conf");
		Path dbreaderMetaDbPath = dbreaderDbDir.resolve("synclite_dbreader_metadata.db");
		Path dbreaderLoggerConf = dbreaderDbDir.resolve("synclite_logger.conf");

		try {
			// Clean up any stale dbreader state from a previous run
			if (Files.exists(dbreaderDbDir)) {
				try (java.util.stream.Stream<Path> walk = Files.walk(dbreaderDbDir)) {
					walk.sorted(java.util.Comparator.reverseOrder())
						.map(Path::toFile)
						.forEach(java.io.File::delete);
				}
			}
			if (Files.exists(dbreaderSrcDbDir)) {
				try (java.util.stream.Stream<Path> walk = Files.walk(dbreaderSrcDbDir)) {
					walk.sorted(java.util.Comparator.reverseOrder())
						.map(Path::toFile)
						.forEach(java.io.File::delete);
				}
			}
			Files.createDirectories(dbreaderDbDir);
			Files.createDirectories(dbreaderSrcDbDir);

			// Source SQLite database
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbreaderSrcDbPath);
					Statement stmt = conn.createStatement()) {
				stmt.execute(
					"CREATE TABLE " + dbReaderTable + " (" +
					"id INTEGER PRIMARY KEY, " +
					"col_text TEXT, " +
					"col_varchar VARCHAR(100), " +
					"col_int INTEGER, " +
					"col_smallint SMALLINT, " +
					"col_bigint BIGINT, " +
					"col_real REAL, " +
					"col_double DOUBLE, " +
					"col_float FLOAT, " +
					"col_numeric NUMERIC(10,2), " +
					"col_decimal DECIMAL(8,4), " +
					"col_boolean BOOLEAN, " +
					"col_date DATE, " +
					"col_datetime DATETIME, " +
					"col_timestamp TIMESTAMP, " +
					"col_blob BLOB, " +
					"col_clob CLOB, " +
					"is_deleted INTEGER DEFAULT 0, " +
					"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
				stmt.execute(
					"INSERT INTO " + dbReaderTable + " VALUES(" +
					"1,'hello','varchar_1',10,2,9000000000,1.1,2.2,3.3,12.34,56.78," +
					"1,'2025-01-01','2025-01-01 10:00:00','2025-01-01 10:00:00'," +
					"X'DEADBEEF','clob1',0,'2025-01-01 00:00:01')");
				stmt.execute(
					"INSERT INTO " + dbReaderTable + " VALUES(" +
					"2,'world','varchar_2',20,4,8000000000,4.4,5.5,6.6,78.90,12.34," +
					"0,'2025-02-01','2025-02-01 11:00:00','2025-02-01 11:00:00'," +
					"X'CAFEBABE','clob2',0,'2025-01-01 00:00:02')");
				stmt.execute(
					"INSERT INTO " + dbReaderTable + " VALUES(" +
					"3,'test','varchar_3',30,6,7000000000,7.7,8.8,9.9,11.22,33.44," +
					"1,'2025-03-01','2025-03-01 12:00:00','2025-03-01 12:00:00'," +
					"X'BEEFDEAD','clob3',0,'2025-01-01 00:00:03')");
			}

			// Logger config
			Files.writeString(dbreaderLoggerConf,
				"local-data-stage-directory = " + this.stageDir + "\n" +
				"local-command-stage-directory = " + this.commandDir + "\n" +
				"destination-type = FS\n");

			// DBReader config
			Files.writeString(dbreaderConfigPath,
				"synclite-device-dir = " + dbreaderDbDir + "\n" +
				"synclite-logger-configuration-file = " + dbreaderLoggerConf + "\n" +
				"src-type = SQLITE\n" +
				"src-connection-string = jdbc:sqlite:" + dbreaderSrcDbPath + "\n" +
				"src-connection-timeout-s = 30\n" +
				"src-dbreader-interval-s = 2\n" +
				"src-dbreader-batch-size = 100000\n" +
				"src-dbreader-processors = 1\n" +
				"src-dbreader-method = INCREMENTAL\n" +
				"dbreader-stop-after-first-iteration = false\n" +
				"src-object-type = TABLE\n" +
				"src-default-unique-key-column-list = id\n" +
				"src-default-incremental-key-column-list = updated_at\n" +
				"src-timestamp-incremental-key-initial-value = 0001-01-01 00:00:00\n" +
				"src-default-soft-delete-condition = is_deleted = 1\n" +
				"src-infer-schema-changes = true\n" +
				"src-infer-object-drop = true\n" +
				"dbreader-trace-level = DEBUG\n" +
				"dbreader-update-statistics-interval-s = 5\n" +
				"dbreader-enable-statistics-collector = true\n" +
				"edition = DEVELOPER\n");

			// DBReader metadata DB
			try (Connection metaConn = DriverManager.getConnection("jdbc:sqlite:" + dbreaderMetaDbPath);
					Statement metaStmt = metaConn.createStatement()) {
				metaStmt.execute(
					"CREATE TABLE IF NOT EXISTS src_object_info(" +
					"object_name TEXT PRIMARY KEY, object_type TEXT, " +
					"allowed_columns TEXT, unique_key_columns TEXT, " +
					"incremental_key_columns TEXT, group_name TEXT, " +
					"group_position INTEGER, mask_columns TEXT, " +
					"delete_condition TEXT, select_conditions TEXT, enable INTEGER)");
				metaStmt.execute(
					"CREATE TABLE IF NOT EXISTS src_object_reload_configurations(" +
					"object_name TEXT PRIMARY KEY, " +
					"reload_schema_on_next_restart INT, reload_schema_on_each_restart INT, " +
					"reload_object_on_next_restart INT, reload_object_on_each_restart INT)");

				String allowedCols;
				try (Connection srcConn = DriverManager.getConnection("jdbc:sqlite:" + dbreaderSrcDbPath)) {
					allowedCols = readJdbcSchemaAsJson(srcConn, dbReaderTable);
				}
				metaStmt.execute(
					"INSERT INTO src_object_info VALUES('" + dbReaderTable + "','TABLE','" +
					allowedCols.replace("'", "''") + "'," +
					"'id','updated_at','',1,'','is_deleted = 1','',1)");
				metaStmt.execute(
					"INSERT INTO src_object_reload_configurations VALUES('" +
					dbReaderTable + "',0,0,0,0)");
			}

			// Start dbreader
			startDBReaderJob(dbreaderDbDir, dbreaderConfigPath);
			Thread.sleep(5000);
			this.dbreaderEnabled = true;
			globalTracer.debug("DBReader started and ready");
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to set up and start dbreader job : ", e);
		}
	}

	private final long getQReaderJobPID() throws SyncLiteTestException {
		try {
			long currentJobPID = 0;
			String javaHome = System.getenv("JAVA_HOME");
			String jpsExe = (javaHome != null)
					? javaHome + (isWindows() ? "\\bin\\jps" : "/bin/jps")
					: "jps";
			String[] cmdArray = {jpsExe, "-l", "-m"};
			Process jpsProc = Runtime.getRuntime().exec(cmdArray);
			BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
			String line = stdout.readLine();
			while (line != null) {
				if (line.contains("com.synclite.qreader.Main")) {
					try {
						currentJobPID = Long.valueOf(line.split(" ")[0]);
					} catch (NumberFormatException ignored) {}
				}
				line = stdout.readLine();
			}
			return currentJobPID;
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to get qreader job PID : ", e);
		}
	}

	private final void startQReaderJob(Path dbDir, Path config) throws SyncLiteTestException {
		globalTracer.debug("Starting qreader job");
		try {
			String corePathStr = this.corePath.toString();
			if (isWindows()) {
				String scriptPath = Path.of(corePathStr, "synclite-qreader.bat").toString();
				String[] cmdArray = {scriptPath, "read", "--db-dir", dbDir.toString(), "--config", config.toString()};
				Runtime.getRuntime().exec(cmdArray);
			} else {
				Path scriptPath = Path.of(corePathStr, "synclite-qreader.sh");
				Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
				if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
					perms.add(PosixFilePermission.OWNER_EXECUTE);
					Files.setPosixFilePermissions(scriptPath, perms);
				}
				String[] cmdArray = {scriptPath.toString(), "read", "--db-dir", dbDir.toString(), "--config", config.toString()};
				Runtime.getRuntime().exec(cmdArray);
			}
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to start qreader job : ", e);
		}
	}

	private final void setupAndStartQReaderJob() throws SyncLiteTestException {
		Path qreaderScript = this.corePath.resolve(isWindows() ? "synclite-qreader.bat" : "synclite-qreader.sh");
		if (!Files.exists(qreaderScript)) {
			globalTracer.debug("Skipping qreader setup: qreader script not found at " + qreaderScript);
			return;
		}

		// Start embedded MQTT broker so the test is self-contained
		try {
			Properties brokerProps = new Properties();
			brokerProps.setProperty(BrokerConstants.PORT_PROPERTY_NAME, "1883");
			brokerProps.setProperty(BrokerConstants.HOST_PROPERTY_NAME, "0.0.0.0");
			brokerProps.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "true");
			embeddedMqttBroker = new Server();
			embeddedMqttBroker.startServer(new MemoryConfig(brokerProps));
			globalTracer.debug("Embedded MQTT broker started on port 1883");
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to start embedded MQTT broker: ", e);
		}

		Path qreaderTestRoot = this.dbDir.resolve("testqreader");
		this.qreaderDbDir = qreaderTestRoot.resolve("db");
		this.qreaderConfigPath = qreaderDbDir.resolve("synclite-qreader.conf");
		Path qreaderMetaDbPath = qreaderDbDir.resolve("synclite_qreader_metadata.db");
		this.qreaderLoggerConf = qreaderDbDir.resolve("synclite_logger.conf");
		this.qreaderDeviceDbPath = qreaderDbDir.resolve(QREADER_DEVICE_NAME + ".db");

		try {
			// Clean up any stale qreader state from a previous run
			if (Files.exists(qreaderDbDir)) {
				try (java.util.stream.Stream<Path> walk = Files.walk(qreaderDbDir)) {
					walk.sorted(java.util.Comparator.reverseOrder())
						.map(Path::toFile)
						.forEach(java.io.File::delete);
				}
			}
			Files.createDirectories(qreaderDbDir);

			// Logger config (used by qreader to stage SyncLite devices)
			Files.writeString(qreaderLoggerConf,
				"local-data-stage-directory = " + this.stageDir + "\n" +
				"local-command-stage-directory = " + this.commandDir + "\n" +
				"destination-type = FS\n");

			// QReader config
			Files.writeString(qreaderConfigPath,
				"synclite-device-dir = " + qreaderDbDir + "\n" +
				"synclite-logger-configuration-file = " + qreaderLoggerConf + "\n" +
				"mqtt-broker-url = " + MQTT_BROKER_URL + "\n" +
				"mqtt-qos-level = 1\n" +
				"mqtt-clean-session = true\n" +
				"mqtt-broker-connection-timeout-s = 10\n" +
				"mqtt-broker-connection-retry-interval-s = 2\n" +
				"qreader-synclite-device-type = SQLITE_APPENDER\n" +
				"qreader-map-devices-to-single-synclite-device = true\n" +
				"qreader-default-synclite-device-name = " + QREADER_DEVICE_NAME + "\n" +
				"qreader-ignore-messages-for-undefined-topics = false\n" +
				"qreader-default-synclite-table-name = default_table\n" +
				"qreader-ignore-corrupt_messages = false\n" +
				"qreader-corrupt-messages-synclite-table-name = corrupt_messages\n" +
				"qreader-message-batch-processing = false\n" +
				"qreader-message-batch-flush-interval-ms = 1000\n" +
				"qreader-trace-level = DEBUG\n" +
				"mqtt-message-header-delimiter = /\n" +
				"src-message-field-delimiter = ,\n" +
				"src-message-format = CSV\n");

			// Pre-populate metadata DB: register the topic so qreader knows which table to write to
			try (Connection metaConn = DriverManager.getConnection("jdbc:sqlite:" + qreaderMetaDbPath);
					Statement metaStmt = metaConn.createStatement()) {
				metaStmt.execute(
					"CREATE TABLE IF NOT EXISTS topic_info (" +
					"topic_name TEXT PRIMARY KEY, " +
					"topic_table_name TEXT, " +
					"topic_field_count INTEGER, " +
					"topic_create_table_sql TEXT, " +
					"topic_table_column_list TEXT, " +
					"enable INTEGER DEFAULT 1)");
				metaStmt.execute(
					"INSERT INTO topic_info(topic_name, topic_table_name, topic_field_count, " +
					"topic_create_table_sql, topic_table_column_list, enable) VALUES (" +
					"'" + QREADER_TABLE + "','" + QREADER_TABLE + "',2," +
					"'CREATE TABLE IF NOT EXISTS " + QREADER_TABLE + "(col1 TEXT, col2 TEXT)'," +
					"'device_name,col1,col2',1)");
			}

			startQReaderJob(qreaderDbDir, qreaderConfigPath);
			// Give qreader time to connect to broker and register subscriptions
			Thread.sleep(5000);
			this.qreaderEnabled = true;
			globalTracer.debug("QReader started and ready");
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to set up and start qreader job : ", e);
		}
	}

	private final void stopQReaderJob() throws SyncLiteTestException {
		globalTracer.debug("Stopping qreader job");
		try {
			long currentJobPID = getQReaderJobPID();
			if (currentJobPID > 0) {
				if (isWindows()) {
					String[] cmdArray = {"taskkill", "/F", "/PID", String.valueOf(currentJobPID)};
					Runtime.getRuntime().exec(cmdArray);
				} else {
					String[] cmdArray = {"kill", "-9", String.valueOf(currentJobPID)};
					Runtime.getRuntime().exec(cmdArray);
				}
			}
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to stop qreader job : ", e);
		}
		// Stop embedded MQTT broker
		if (embeddedMqttBroker != null) {
			embeddedMqttBroker.stopServer();
			embeddedMqttBroker = null;
			globalTracer.debug("Embedded MQTT broker stopped");
		}
	}

	private final void stopJobs() throws SyncLiteTestException {
		//Stop synclite-db
		try {
			//
			{
				//Get current job PID if running
				long currentJobPID = 0;
				Process jpsProc = Runtime.getRuntime().exec("jps -l -m");
				BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
				String line = stdout.readLine();
				while (line != null) {
					if (line.contains("com.synclite.db.Main")) {
						currentJobPID = Long.valueOf(line.split(" ")[0]);
					}
					line = stdout.readLine();
				}
				//stdout.close();

				//Kill job if found

				if(currentJobPID > 0) {
					if (isWindows()) {
						Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
					} else {
						Runtime.getRuntime().exec("kill -9 " + currentJobPID);
					}
				}
			}

			//
			//Stop consolidator job
			//
			{
				//Get current job PID if running
				long currentJobPID = 0;
				Process jpsProc = Runtime.getRuntime().exec("jps -l -m");
				BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
				String line = stdout.readLine();
				while (line != null) {
					if (line.contains("com.synclite.consolidator.Main")) {
						currentJobPID = Long.valueOf(line.split(" ")[0]);
					}
					line = stdout.readLine();
				}
				//stdout.close();

				//Kill job if found

				if(currentJobPID > 0) {
					if (isWindows()) {
						Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
					} else {
						Runtime.getRuntime().exec("kill -9 " + currentJobPID);
					}
				}

			}
		} catch (Exception e) {
			//Ignore
		}
		// Stop qreader job if it was started
		if (qreaderEnabled) {
			try {
				stopQReaderJob();
			} catch (Exception e) {
				//Ignore
			}
		}
		// Stop dbreader job if it was started
		if (dbreaderEnabled) {
			try {
				stopDBReaderJob();
			} catch (Exception e) {
				//Ignore
			}
		}
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
			this.dstTablePrefix = "";
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

	/*
	public void runTestsSerial() throws SyncLiteTestException {
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

		testSQLiteStorePreparedStmtBasic();
		testSQLiteStoreFatTableAutoArgInlining();
		testSQLiteStoreFatTableFixedInlinedArgs();
		testSQLiteStoreInsertWithColList();

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
	 */

	public void runTests() throws SyncLiteTestException, InterruptedException, ExecutionException {
		       createMockDevice();
		       setupAndStartDBReaderJob();
		       setupAndStartQReaderJob();
		       // Add test method calls here
		       // List of test method references
		       List<Callable<Void>> testTasks = Arrays.asList(
			       () -> { testSQLiteStmtBasic(); return null; },
			       () -> { testSQLitePreparedStmtBasic(); return null; },
			       () -> { testSQLiteTableMerge(); return null; },
			       () -> { testSQLiteCommitRollback(); return null; },
			       () -> { testSQLiteFatTableAutoArgInlining(); return null; },
			       () -> { testSQLiteFatTableFixedInlinedArgs(); return null; },
			       () -> { testDuckDBStmtBasic(); return null; },
			       () -> { testDuckDBPreparedStmtBasic(); return null; },
			       () -> { testDuckDBCommitRollback(); return null; },
			       () -> { testDerbyStmtBasic(); return null; },
			       () -> { testDerbyPreparedStmtBasic(); return null; },
			       () -> { testDerbyCommitRollback(); return null; },
			       () -> { testH2StmtBasic(); return null; },
			       () -> { testH2PreparedStmtBasic(); return null; },
			       () -> { testH2CommitRollback(); return null; },
			       () -> { testHyperSQLStmtBasic(); return null; },
			       () -> { testHyperSQLPreparedStmtBasic(); return null; },
			       () -> { testHyperSQLCommitRollback(); return null; },
			       () -> { testSQLiteInSyncLiteDB(); return null; },
			       () -> { testDuckDBInSyncLiteDB(); return null; },
			       () -> { testH2InSyncLiteDB(); return null; },
			       () -> { testDerbyInSyncLiteDB(); return null; },
			       () -> { testHyperSQLInSyncLiteDB(); return null; },
			       () -> { testSQLiteAppenderInSyncLiteDB(); return null; },
			       () -> { testDuckDBAppenderInSyncLiteDB(); return null; },
			       () -> { testH2AppenderInSyncLiteDB(); return null; },
			       () -> { testDerbyAppenderInSyncLiteDB(); return null; },
			       () -> { testHyperSQLAppenderInSyncLiteDB(); return null; },
			       () -> { testSQLiteStoreAPIBasic(); return null; },
			       () -> { testStreamingPreparedStmtBasic(); return null; },
			       () -> { testStreamingAPIBasic(); return null; },
			       () -> { testJedisAPIBasic(); return null; },
			       () -> { testKafkaProducerAPIBasic(); return null; },
			       () -> { testStreamingInSyncLiteDB(); return null; },
			       () -> { testQReader(); return null; }
		       );

		       ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
		       List<Future<Void>> futures = executorService.invokeAll(testTasks);
		       for (Future<Void> future : futures) {
			       future.get();  
		       }
		       // Shut down the executor
		       executorService.shutdown();        
		       // Run tests which cannot be in parallel execution
		       // These tests restart the consolidator process and hence must be executed sequentially at the end.
		       testDBReader();
		       testSQLiteReinitializeDevice();
		   }
	//
	//=================================================

	private void createMockDevice() throws SyncLiteTestException {
		globalTracer.error("Testing mock device");
		String testName = "mockTest";
		try {
			Path testDBPath = dbDir.resolve(testName + ".db");
			SQLite.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite:" + dbDir.resolve(testDBPath);

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
		globalTracer.setLevel(Level.DEBUG);
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("SyncLiteValidatorTracer");
		fa.setFile(workDir.resolve("synclite_validator.trace").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] [%t] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setAppend(true);
		fa.activateOptions();
		globalTracer.addAppender(fa);
	}

	private final synchronized void preTest(String testName) throws SyncLiteTestException {
		String startTimeStr = Instant.now().toString().replace("T", " ").replace("Z", "");
		lastTestStartTime.set(System.currentTimeMillis());
		try {
			insertPstmtValidatorDB.clearBatch();
			insertPstmtValidatorDB.setString(1, testName);
			insertPstmtValidatorDB.setString(2, startTimeStr);
			insertPstmtValidatorDB.setString(3, null);
			insertPstmtValidatorDB.setLong(4, 0);
			insertPstmtValidatorDB.setString(5, "RUNNING");
			insertPstmtValidatorDB.execute();
			globalTracer.debug("Started Test : " + testName);
		} catch (SQLException e) {
			throw new SyncLiteTestException("Failed to excecute test " + testName + " in preTest phase : ", e);
		}
	}

	private final synchronized void postTest(String testName, String executionStatus) throws SyncLiteTestException {
		String endTimeStr = Instant.now().toString().replace("T", " ").replace("Z", "");
		lastTestFinishTime.set(System.currentTimeMillis());
		long testExecutionTime = lastTestFinishTime.get() - lastTestStartTime.get();
		try {
			updatePstmtValidatorDB.clearBatch();
			updatePstmtValidatorDB.setString(1, endTimeStr);
			updatePstmtValidatorDB.setLong(2, testExecutionTime);
			updatePstmtValidatorDB.setString(3, executionStatus);
			updatePstmtValidatorDB.setString(4, testName);
			updatePstmtValidatorDB.execute();
			globalTracer.debug("Finished Test : " + testName);
		} catch (SQLException e) {
			throw new SyncLiteTestException("Failed to excecute test " + testName + " in postTest phase : ", e);
		}
	}

	private final void verifyDataWithDevice(String deviceName, DeviceType deviceType, Path devicePath, String tabName, List<String> cols, List<String> orderCols) throws SyncLiteTestException, InterruptedException {
		globalTracer.debug("Verifying data for table " + tabName + " in destination with respect to device :" + deviceName);

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
			props.setProperty("readonly", "true");
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
		globalTracer.debug("Verifying data for table :" + tabName);
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
		globalTracer.debug("Result verification dump for SQL : " + sql);
		globalTracer.debug("Expected Rows : ");
		for (String s : expectedRows) {
			globalTracer.debug(s);
		}
		globalTracer.debug("Current Rows : ");
		for (String s : currentRows) {
			globalTracer.debug(s);
		}
	}

	/**
	 * Scans every subdirectory of stageDir and dumps the last {@code lines} rows
	 * of the commandlog from each {@code 0.sqllog} found.  Useful for diagnosing
	 * which transactions were actually staged (including any that should have been
	 * suppressed by a rollback).
	 */
	private final void dumpStageDirCommandLog(String label, int lines) {
		try {
			globalTracer.error("[" + label + "] === Stage commandlog dump ===");
			if (!Files.exists(stageDir)) {
				globalTracer.error("[" + label + "] stageDir does not exist: " + stageDir);
				return;
			}
			try (java.util.stream.Stream<Path> dirs = Files.list(stageDir)) {
				dirs.filter(Files::isDirectory).forEach(devDir -> {
					Path sqllog = devDir.resolve("0.sqllog");
					if (!Files.exists(sqllog)) return;
					try (java.sql.Connection c = DriverManager.getConnection("jdbc:sqlite:" + sqllog);
							java.sql.Statement s = c.createStatement()) {
						// total entries
						long cnt = 0;
						try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM commandlog")) {
							if (rs.next()) cnt = rs.getLong(1);
						}
						globalTracer.error("[" + label + "] Stage dir: " + devDir.getFileName() + " | commandlog total rows: " + cnt);
						// last N rows
						try (ResultSet rs = s.executeQuery(
								"SELECT change_number, commit_id, sql FROM commandlog ORDER BY change_number DESC LIMIT " + lines)) {
							while (rs.next()) {
								globalTracer.error("[" + label + "]   change=" + rs.getLong(1)
									+ " commit_id=" + rs.getLong(2)
									+ " sql=" + rs.getString(3));
							}
						}
					} catch (Exception e2) {
						globalTracer.error("[" + label + "] Failed to query commandlog in " + sqllog + ": " + e2.getMessage());
					}
				});
			}
		} catch (Exception e) {
			globalTracer.error("[" + label + "] dumpStageDirCommandLog failed: " + e.getMessage());
		}
	}

	/**
	 * Dumps the last {@code lines} lines of the consolidator trace file.
	 */
	private final void dumpConsolidatorTrace(String label, int lines) {
		try {
			Path tracePath = workDir.resolve("synclite_consolidator.trace");
			if (!Files.exists(tracePath)) {
				globalTracer.error("[" + label + "] Consolidator trace not found: " + tracePath);
				return;
			}
			List<String> all = Files.readAllLines(tracePath);
			int from = Math.max(0, all.size() - lines);
			globalTracer.error("[" + label + "] === Last " + lines + " lines of consolidator trace ===");
			for (String ln : all.subList(from, all.size())) {
				globalTracer.error("[" + label + "] " + ln);
			}
		} catch (Exception e) {
			globalTracer.error("[" + label + "] dumpConsolidatorTrace failed: " + e.getMessage());
		}
	}

	private final void waitForConsolidationStartup() throws SyncLiteTestException {
		globalTracer.debug("Waiting for data consolidation startup");
		try {
			long waited = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				try {
					String dstQuery = "SELECT commit_id FROM " + dstTablePrefix + "synclite_metadata";
					dstDBReader.readScalarLong(dstQuery);
					globalTracer.debug("Verified Data Consolidation startup");
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
		globalTracer.debug("Waiting for data consolidation of the executed workload");
		try {
			long waited = 0;
			long deviceCommitID = 0;
			long dstCommitID = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				DBReader deviceDBReader;
				Properties props = new Properties();
				if (deviceType == DeviceType.DUCKDB || deviceType == DeviceType.DUCKDB_APPENDER) {
					props.setProperty("duckdb.read_only", "true");
					deviceDBReader = new DBReader(DstType.DUCKDB, "jdbc:duckdb:" + devicePath.toString(), props, this.globalTracer);
				} else if (deviceType == DeviceType.DERBY || deviceType == DeviceType.DERBY_APPENDER) {
					props.setProperty("readonly", "true");
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

	private final void waitForConsolidationOfSyncLiteDB(String deviceName, DeviceType deviceType, Path devicePath) throws SyncLiteTestException {
		globalTracer.debug("Waiting for data consolidation of the executed workload");
		try {
			long waited = 0;
			long deviceCommitID = 0;
			long dstCommitID = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				try {
					SyncLiteDBResult r = executeSQL(deviceName, null, DEVICE_COMMIT_ID_READER_QUERY, null);
					if (r.resultSet == null) {
						globalTracer.debug("Failed to read max commit id from SyncLiteDB : " + r.message);
					} else if (r.resultSet.length() != 1) {
						globalTracer.debug("max commit id is missing in SyncLiteDB");
					} else {
						JSONObject o = r.resultSet.getJSONObject(0);
						Iterator<String> keys = o.keys();
						if (keys.hasNext()) {
							String key = keys.next();
							deviceCommitID = o.getLong(key);
						}
					}
				} catch (SQLException e) {
					deviceCommitID = 0;
					globalTracer.debug("Failed to read max commit id from SyncLiteDB : " + e.getMessage(), e);
				}

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

	private final String readJdbcSchemaAsJson(Connection conn, String tableName) throws Exception {
		DatabaseMetaData meta = conn.getMetaData();
		StringBuilder sb = new StringBuilder("[");
		boolean first = true;
		try (ResultSet cols = meta.getColumns(null, null, tableName, null)) {
			while (cols.next()) {
				String colName    = cols.getString("COLUMN_NAME");
				String typeName   = cols.getString("TYPE_NAME").toUpperCase();
				int    colSize    = cols.getInt("COLUMN_SIZE");
				int    decDigits  = cols.getInt("DECIMAL_DIGITS");
				String isNullable = cols.getString("IS_NULLABLE");

				StringBuilder typeBldr = new StringBuilder(typeName);
				if (typeName.equals("DECIMAL") && colSize > 0) {
					typeBldr.append("(").append(colSize);
					if (decDigits > 0) typeBldr.append(", ").append(decDigits);
					typeBldr.append(")");
				} else if ((typeName.equals("VARCHAR") || typeName.equals("CHAR") ||
						typeName.equals("NCHAR") || typeName.equals("NVARCHAR")) && colSize > 0) {
					typeBldr.append("(").append(colSize).append(")");
				}
				String nullable = (isNullable.equalsIgnoreCase("NO") ||
						isNullable.equalsIgnoreCase("N")  ||
						isNullable.equals("0")            ||
						isNullable.equalsIgnoreCase("FALSE") ||
						isNullable.equalsIgnoreCase("NOT NULL"))
						? "NOT NULL" : "NULL";
				typeBldr.append(" ").append(nullable);

				if (!first) sb.append(", ");
				first = false;
				sb.append("\"").append(colName).append(" ").append(typeBldr).append("\"");
			}
		}
		sb.append("]");
		return sb.toString();
	}

	private final void waitForDbreaderReplicationRowCount(String table, long expected) throws SyncLiteTestException {
		globalTracer.debug("Waiting for dbreader replication row count (" + expected + ") for table: " + table);
		try {
			long waited = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				try {
					long count = dstDBReader.readScalarLong("SELECT COUNT(*) FROM " + this.dstTablePrefix + table);
					if (count == expected) {
						globalTracer.debug("Verified row count " + expected + " for table: " + table);
						return;
					}
				} catch (SyncLiteTestException e) {
					// table may not exist yet in destination
				}
				Thread.sleep(CONSOLIDATION_CHECK_INTERVAL);
				waited += CONSOLIDATION_CHECK_INTERVAL;
			}
			long actual = -1;
			try {
				actual = dstDBReader.readScalarLong("SELECT COUNT(*) FROM " + this.dstTablePrefix + table);
			} catch (Exception ignored) {}
			throw new SyncLiteTestException("Timed out waiting for " + expected + " rows in " + table + "; actual=" + actual);
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	private final void waitForDbreaderReplicationValue(String table, String column, String idValue, String expected) throws SyncLiteTestException {
		globalTracer.debug("Waiting for dbreader replication value '" + expected + "' in " + table + "." + column + " WHERE id=" + idValue);
		try {
			long waited = 0;
			while (waited <= CONSOLIDATION_WAIT_DURATION_MS) {
				try {
					List<String> rows = dstDBReader.readRows(
						"SELECT " + column + " FROM " + this.dstTablePrefix + table + " WHERE id = " + idValue);
					if (!rows.isEmpty() && expected.equals(rows.get(0))) {
						globalTracer.debug("Verified value '" + expected + "' in " + table + "." + column);
						return;
					}
				} catch (SyncLiteTestException e) {
					// table may not exist yet
				}
				Thread.sleep(CONSOLIDATION_CHECK_INTERVAL);
				waited += CONSOLIDATION_CHECK_INTERVAL;
			}
			throw new SyncLiteTestException("Timed out waiting for value '" + expected + "' in " + table + "." + column + " WHERE id=" + idValue);
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	private final void testSQLiteStmtBasic() throws SyncLiteTestException {		
		String testName = "testSQLiteStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
					stmt.execute("INSERT INTO " + tableName + " VALUES(1, 1.1, '1', '1', '1')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(2, 2.2, '2', '2', '2')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(4, 4.4, '4', '4', '4')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5.5, '5', '5', '5')");

					stmt.execute("UPDATE " + tableName + " SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3', col5 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM " + tableName + " WHERE col1 = 5");
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
			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, tableName, cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}


	private final void testSQLitePreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testSQLitePreparedStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)")) {
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

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tableName + " SET col1 = ?, col2 = ?, col3 = ?, col4 = ?, col5 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setBytes(5, "3".getBytes());
					pstmt.setInt(6, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE col1 = ?")) {
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

			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, tableName, cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDuckDBStmtBasic() throws SyncLiteTestException {		
		String testName = "testDuckDBStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 TEXT)");
					stmt.execute("INSERT INTO " + tableName + " VALUES(1, 1.1, '1', '1')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(2, 2.2, '2', '2')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(4, 4.4, '4', '4')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5.5, '5', '5')");

					stmt.execute("UPDATE " + tableName + " SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM " + tableName + " WHERE col1 = 5");
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
			verifyDataWithDevice(testName, DeviceType.DUCKDB, testDBPath, tableName, cols, orderCols);

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
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 TEXT)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?)")) {
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

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tableName + " SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE col1 = ?")) {
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

			verifyDataWithDevice(testName, DeviceType.DUCKDB, testDBPath, tableName, cols, orderCols);

			DuckDB.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDerbyStmtBasic() throws SyncLiteTestException {		
		String testName = "testDerbyStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
					stmt.execute("INSERT INTO " + tableName + " VALUES(1, 1.1, '1', '1')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(2, 2.2, '2', '2')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(4, 4.4, '4', '4')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5.5, '5', '5')");

					stmt.execute("UPDATE " + tableName + " SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM " + tableName + " WHERE col1 = 5");
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
			verifyDataWithDevice(testName, DeviceType.DERBY, testDBPath, tableName, cols, orderCols);

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
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?)")) {
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

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tableName + " SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE col1 = ?")) {
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

			verifyDataWithDevice(testName, DeviceType.DERBY, testDBPath, tableName, cols, orderCols);

			Derby.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}


	private final void testH2StmtBasic() throws SyncLiteTestException {		
		String testName = "testH2StmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
					stmt.execute("INSERT INTO " + tableName + " VALUES(1, 1.1, '1', '1')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(2, 2.2, '2', '2')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(4, 4.4, '4', '4')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5.5, '5', '5')");

					stmt.execute("UPDATE " + tableName + " SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM " + tableName + " WHERE col1 = 5");
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
			verifyDataWithDevice(testName, DeviceType.H2, testDBPath, tableName, cols, orderCols);

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
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50), col4 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?)")) {
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

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tableName + " SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3.3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE col1 = ?")) {
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

			verifyDataWithDevice(testName, DeviceType.H2, testDBPath, tableName, cols, orderCols);

			H2.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}



	private final void testHyperSQLStmtBasic() throws SyncLiteTestException {		
		String testName = "testHyperSQLStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 INTEGER, col3 VARCHAR(50), col4 VARCHAR(50))");
					stmt.execute("INSERT INTO " + tableName + " VALUES(1, 1, '1', '1')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(2, 2, '2', '2')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(4, 4, '4', '4')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5, '5', '5')");

					stmt.execute("UPDATE " + tableName + " SET col1 = 3, col2 = 3, col3 = '3', col4 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM " + tableName + " WHERE col1 = 5");
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
			verifyDataWithDevice(testName, DeviceType.HYPERSQL, testDBPath, tableName, cols, orderCols);

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
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 INTEGER, col3 VARCHAR(50), col4 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?)")) {
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

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tableName + " SET col1 = ?, col2 = ?, col3 = ?, col4 = ? WHERE col1 = ?")) {
					pstmt.setInt(1, 3);
					pstmt.setDouble(2, 3);
					pstmt.setString(3, "3");
					pstmt.setString(4, "3");
					pstmt.setInt(5, 4);

					pstmt.execute();
				}

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE col1 = ?")) {
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

			verifyDataWithDevice(testName, DeviceType.HYPERSQL, testDBPath, tableName, cols, orderCols);

			HyperSQL.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteStorePreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testSQLiteStorePreparedStmtBasic";
		String tableName = testName;
		String checkpointTableName = testName + "_dummy";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLiteStore.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite_store:" + dbDir.resolve(testDBPath);

			//Test a basic scenario 
			//1. create a table with an INTEGER, FLOATING POINT, TEXT and BLOB column
			//2. INSERT few rows using PreparedStatament
			//3. UPDATE a row using PreparedStatement
			//4. DELETE a row using PreparedStatement
			//5. Validate data in db file with that of consolidated db.			

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)")) {
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
					stmt.execute("CREATE TABLE " + checkpointTableName + "(col1 int)");
				}				
			}

			waitForConsolidation(testName, DeviceType.SQLITE_STORE, testDBPath);

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

			verifyData(expectedRows, tableName, cols, orderCols);

			SQLiteStore.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testStreamingPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testStreamingPreparedStmtBasic";
		String tableName = testName;
		String checkpointTableName = testName + "_dummy";

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)")) {
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
					stmt.execute("CREATE TABLE " + checkpointTableName + "(col1 int)");
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

			verifyData(expectedRows, tableName, cols, orderCols);

			Streaming.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteStoreAPIBasic() throws SyncLiteTestException {
		String testName = "testSQLiteStoreAPIBasic";
		String tableName = testName + "_tbl";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			SQLiteStore.initialize(testDBPath, loggerConfig, testName);

			try (SyncLiteStore store = SQLiteStore.open(testDBPath)) {
				java.util.LinkedHashMap<String, String> colsDef = new java.util.LinkedHashMap<String, String>();
				colsDef.put("id", "INTEGER PRIMARY KEY");
				colsDef.put("name", "TEXT");
				colsDef.put("score", "INTEGER");
				store.createTable(tableName, colsDef);

				store.insert(tableName, java.util.Map.of("id", 1, "name", "alice", "score", 100));
				store.insert(tableName, java.util.Map.of("id", 2, "name", "bob", "score", 200));
				store.update(tableName, java.util.Map.of("score", 150), java.util.Map.of("name", "alice"));
				store.delete(tableName, java.util.Map.of("name", "bob"));

				List<java.util.Map<String, Object>> batchRows = List.of(
						java.util.Map.of("id", 3, "name", "carol", "score", 300),
						java.util.Map.of("id", 4, "name", "dave", "score", 400));
				store.insertBatch(tableName, batchRows);

				store.updateBatch(tableName,
						List.of(java.util.Map.of("score", 350), java.util.Map.of("score", 450)),
						List.of(java.util.Map.of("name", "carol"), java.util.Map.of("name", "dave")));
				store.deleteBatch(tableName, List.of(java.util.Map.of("name", "dave")));

				store.insert(tableName, java.util.Map.of("id", 5, "name", "eve", "score", 500));

				// Test rollback: rolled-back transaction must NOT appear in destination.
				// Product bug: SyncLiteStore.rollback() currently does not suppress the log entry —
				// the rolled-back row (id=99) is staged with a valid commit_id and replayed by the
				// consolidator. If this test fails, use dumpStageDirCommandLog output to confirm.
				store.setAutoCommit(false);
				store.insert(tableName, java.util.Map.of("id", 99, "name", "temp", "score", 999));
				store.rollback();
				store.setAutoCommit(true);
				globalTracer.info("[" + testName + "] Rolled back temp row (id=99). It must NOT appear in destination.");
			}

			try {
				waitForConsolidation(testName, DeviceType.SQLITE_STORE, testDBPath);
			} catch (SyncLiteTestException waitEx) {
				globalTracer.error("[" + testName + "] waitForConsolidation timed out — dumping diagnostics");
				dumpStageDirCommandLog(testName, 30);
				dumpConsolidatorTrace(testName, 40);
				throw waitEx;
			}

			List<String> cols = new ArrayList<String>();
			cols.add("id");
			cols.add("name");
			cols.add("score");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("id");

			// Expected: committed rows only. id=99 (rolled back) must be absent.
			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|alice|150");
			expectedRows.add("3|carol|350");
			expectedRows.add("5|eve|500");

			verifyData(expectedRows, tableName, cols, orderCols);

			SQLiteStore.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(), e);
			postTest(testName, "FAIL");
		}
	}

	private final void testStreamingAPIBasic() throws SyncLiteTestException {
		String testName = "testStreamingAPIBasic";
		String tableName = testName + "_tbl";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			Streaming.initialize(testDBPath, loggerConfig, testName);

			try (SyncLiteStream stream = SyncLiteStream.open(testDBPath)) {
				java.util.LinkedHashMap<String, String> colsDef = new java.util.LinkedHashMap<String, String>();
				colsDef.put("id", "INTEGER PRIMARY KEY");
				colsDef.put("event_type", "TEXT");
				colsDef.put("user_name", "TEXT");
				stream.createTable(tableName, colsDef);

				List<java.util.Map<String, Object>> batchRows = List.of(
						java.util.Map.of("id", 1, "event_type", "click", "user_name", "alice"),
						java.util.Map.of("id", 2, "event_type", "view", "user_name", "bob"),
						java.util.Map.of("id", 3, "event_type", "purchase", "user_name", "carol"));
				stream.insertBatch(tableName, batchRows);
			}

			waitForConsolidation(testName, DeviceType.STREAMING, testDBPath);

			List<String> cols = new ArrayList<String>();
			cols.add("id");
			cols.add("event_type");
			cols.add("user_name");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("id");

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|click|alice");
			expectedRows.add("2|view|bob");
			expectedRows.add("3|purchase|carol");

			verifyData(expectedRows, tableName, cols, orderCols);

			Streaming.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(), e);
			postTest(testName, "FAIL");
		}
	}

	private final void testJedisAPIBasic() throws SyncLiteTestException {
		String testName = "testJedisAPIBasic";
		String keyPrefix = testName + ":";
		com.github.fppt.jedismock.RedisServer redisServer = null;

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");
			SQLiteStore.initialize(testDBPath, loggerConfig, testName);

			redisServer = com.github.fppt.jedismock.RedisServer.newRedisServer();
			redisServer.start();

			String redisHost = redisServer.getHost();
			int redisPort = redisServer.getBindPort();

			try (SyncLiteStore store = SQLiteStore.open(testDBPath);
					Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
				jedis.set(keyPrefix + "k1", "v1");
				jedis.rpush(keyPrefix + "list1", "a", "b");
				// Test hash operations — jedis_hashes has composite PK (hash_key, field).
				// Known issue: consolidator throws "table has more than one primary key" when
				// seeding in-memory replica for jedis_hashes. dumpConsolidatorTrace will capture
				// the exact exception if waitForConsolidation times out.
				jedis.hset(keyPrefix + "hash1", "field1", "value1");
				jedis.hset(keyPrefix + "hash1", "field2", "value2");
			}

			try {
				waitForConsolidation(testName, DeviceType.SQLITE_STORE, testDBPath);
			} catch (SyncLiteTestException waitEx) {
				globalTracer.error("[" + testName + "] waitForConsolidation timed out — dumping diagnostics");
				dumpStageDirCommandLog(testName, 30);
				dumpConsolidatorTrace(testName, 60);
				throw waitEx;
			}

			List<String> stringCols = new ArrayList<String>();
			stringCols.add("key");
			stringCols.add("value");

			List<String> listCols = new ArrayList<String>();
			listCols.add("key");
			listCols.add("idx");
			listCols.add("value");

			List<String> hashCols = new ArrayList<String>();
			hashCols.add("hash_key");
			hashCols.add("field");
			hashCols.add("value");

			List<String> orderByKey = new ArrayList<String>();
			orderByKey.add("key");

			List<String> orderByList = new ArrayList<String>();
			orderByList.add("key");
			orderByList.add("idx");

			List<String> orderByHash = new ArrayList<String>();
			orderByHash.add("hash_key");
			orderByHash.add("field");

			List<String> expectedStringRows = new ArrayList<String>();
			expectedStringRows.add(keyPrefix + "k1|v1");

			List<String> expectedListRows = new ArrayList<String>();
			expectedListRows.add(keyPrefix + "list1|0|a");
			expectedListRows.add(keyPrefix + "list1|1|b");

			List<String> expectedHashRows = new ArrayList<String>();
			expectedHashRows.add(keyPrefix + "hash1|field1|value1");
			expectedHashRows.add(keyPrefix + "hash1|field2|value2");

			verifyData(expectedStringRows, "jedis_strings", stringCols, orderByKey);
			verifyData(expectedListRows, "jedis_lists", listCols, orderByList);
			verifyData(expectedHashRows, "jedis_hashes", hashCols, orderByHash);

			SQLiteStore.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(), e);
			postTest(testName, "FAIL");
		} finally {
			if (redisServer != null) {
				try {
					redisServer.stop();
				} catch (Exception ignored) {
				}
			}
		}
	}

	private final void testKafkaProducerAPIBasic() throws SyncLiteTestException {
		String testName = "testKafkaProducerAPIBasic";
		String topicName = testName + "_topic";
		String kafkaDeviceName = "default";

		try {
			preTest(testName);

			Path testDBDir = dbDir.resolve(testName);
			Files.createDirectories(testDBDir);

			Properties props = new Properties();
			props.setProperty("device-path", testDBDir.toString());
			props.setProperty("device-name", testName);
			props.setProperty("local-data-stage-directory", stageDir.toString());
			props.setProperty("destination-type", "FS");

			try (KafkaProducer producer = new KafkaProducer(props)) {
				producer.send(new org.apache.kafka.clients.producer.ProducerRecord<String, String>(topicName, "k1", "v1")).get();
				producer.send(new org.apache.kafka.clients.producer.ProducerRecord<String, String>(topicName, "k2", "v2")).get();
				producer.send(new org.apache.kafka.clients.producer.ProducerRecord<String, String>(topicName, "k3", "v3")).get();
				producer.flush();
			}

			Path deviceFilePath = testDBDir.resolve("default.db");
			waitForConsolidation(kafkaDeviceName, DeviceType.STREAMING, deviceFilePath);

			List<String> cols = new ArrayList<String>();
			cols.add("key");
			cols.add("value");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("key");

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("k1|v1");
			expectedRows.add("k2|v2");
			expectedRows.add("k3|v3");

			verifyData(expectedRows, topicName, cols, orderCols);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(), e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testSQLiteAppenderPreparedStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)")) {
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

			verifyDataWithDevice(testName, DeviceType.SQLITE_APPENDER, testDBPath, tableName, cols, orderCols);

			SQLiteAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testDuckDBAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testDuckDBAppenderPreparedStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT)");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?)")) {
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

			verifyDataWithDevice(testName, DeviceType.DUCKDB_APPENDER, testDBPath, tableName, cols, orderCols);

			DuckDBAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testDerbyAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testDerbyAppenderPreparedStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?)")) {
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

			verifyDataWithDevice(testName, DeviceType.DERBY_APPENDER, testDBPath, tableName, cols, orderCols);

			DerbyAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testH2AppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testH2AppenderPreparedStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?)")) {
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

			verifyDataWithDevice(testName, DeviceType.H2_APPENDER, testDBPath, tableName, cols, orderCols);

			H2Appender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testHyperSQLAppenderPreparedStmtBasic() throws SyncLiteTestException {		
		String testName = "testHyperSQLAppenderPreparedStmtBasic";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 INTEGER, col3 VARCHAR(50))");
				}
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?)")) {
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

			verifyDataWithDevice(testName, DeviceType.HYPERSQL_APPENDER, testDBPath, tableName, cols, orderCols);

			HyperSQLAppender.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}


	private final void testSQLiteStoreFatTableAutoArgInlining() throws SyncLiteTestException {
		String testName = "testSQLiteStoreFatTableAutoArgInlining";

		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLiteStore.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite_store:" + dbDir.resolve(testDBPath);

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
					stmt.execute("CREATE TABLE " + testName + "_dummy(col1 int)");
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE_STORE, testDBPath);

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

			SQLiteStore.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private final void testSQLiteStoreFatTableFixedInlinedArgs() throws SyncLiteTestException {
		String testName = "testSQLiteStoreFatTableFixedInlinedArgs";

		try {
			preTest(testName);

			SyncLiteOptions options = SyncLiteOptions.loadFromFile(loggerConfig);
			options.setLogMaxInlineArgs(50);
			options.setDeviceName(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLiteStore.initialize(testDBPath, options);
			String testDBURL = "jdbc:synclite_sqlite_store:" + dbDir.resolve(testDBPath);

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
					stmt.execute("CREATE TABLE " + testName + "_dummy(col1 int)");
				}				

			}

			waitForConsolidation(testName, DeviceType.SQLITE_STORE, testDBPath);

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

			SQLiteStore.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			globalTracer.error("Details : " + e.getMessage(),  e);
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
			Class.forName("io.synclite.logger.SQLiteStore");
			Class.forName("io.synclite.logger.Streaming");
			runTests();
			stopJobs();
		} catch (Exception e) {
			globalTracer.error("ERROR : ", e);
			System.out.println("ERROR : " + e);
			System.exit(1);
		}
	}


	private final void testSQLiteCallback() throws SyncLiteTestException {		
		String testName = "testSQLiteCallback";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
					stmt.execute("INSERT INTO " + tableName + " VALUES(1, 1.1, '1', '1', '1')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(2, 2.2, '2', '2', '2')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(4, 4.4, '4', '4', '4')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5.5, '5', '5', '5')");
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}


	private final void testDBReader() throws SyncLiteTestException {
		String testName = "testDBReader";
		String dbReaderTable = "dbreadertable";
		Path runtimeConsolidatorConfigPath = this.workDir.resolve("synclite_consolidator.conf");
		String idempotentPropertyName = "dst-idempotent-data-ingestion-1";
		boolean idempotencyToggled = false;

		if (!this.dbreaderEnabled) {
			globalTracer.debug("Skipping " + testName + ": dbreader not started");
			return;
		}

		try {
			preTest(testName);

			// The dbreader incremental flow emits INSERT records for changed rows.
			// In consolidation mode this can cause PK conflicts unless idempotent ingestion is enabled.
			if (Files.exists(runtimeConsolidatorConfigPath)) {
				upsertConfigProperty(runtimeConsolidatorConfigPath, idempotentPropertyName, "true");
				restartSyncConsolidatorJob();
				waitForConsolidationStartup();
				idempotencyToggled = true;
			} else {
				globalTracer.debug("Skipping idempotency toggle in " + testName + ": config file missing at " + runtimeConsolidatorConfigPath);
			}

			// ── Phase 1: 3 initial rows replicated ───────────────────────────
			waitForDbreaderReplicationRowCount(dbReaderTable, 3);
			// Spot-check key columns on row id=1
			List<String> colText = dstDBReader.readRows(
					"SELECT col_text FROM " + this.dstTablePrefix + dbReaderTable + " WHERE id = 1");
			if (colText.isEmpty() || !"hello".equals(colText.get(0))) {
				throw new SyncLiteTestException("Phase 1: expected col_text='hello' for id=1, got: " + colText);
			}
			List<String> colBigint = dstDBReader.readRows(
					"SELECT col_bigint FROM " + this.dstTablePrefix + dbReaderTable + " WHERE id = 1");
			if (colBigint.isEmpty() || !"9000000000".equals(colBigint.get(0))) {
				throw new SyncLiteTestException("Phase 1: expected col_bigint=9000000000 for id=1, got: " + colBigint);
			}

			// ── Phase 2: UPDATE row id=2 ─────────────────────────────────────
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbreaderSrcDbPath);
					Statement stmt = conn.createStatement()) {
				stmt.execute(
					"UPDATE " + dbReaderTable + " SET " +
					"col_text = 'updated', col_int = 999, col_real = 9.99, " +
					"col_boolean = 0, col_clob = 'updated_clob', " +
					"updated_at = '2025-04-01 00:00:10' WHERE id = 2");
			}
			waitForDbreaderReplicationValue(dbReaderTable, "col_text", "2", "updated");
			List<String> colInt = dstDBReader.readRows(
					"SELECT col_int FROM " + this.dstTablePrefix + dbReaderTable + " WHERE id = 2");
			if (colInt.isEmpty() || !"999".equals(colInt.get(0))) {
				throw new SyncLiteTestException("Phase 2: expected col_int=999 for id=2, got: " + colInt);
			}

			// ── Phase 3: Soft-DELETE row id=3 ────────────────────────────────
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbreaderSrcDbPath);
					Statement stmt = conn.createStatement()) {
				stmt.execute(
					"UPDATE " + dbReaderTable + " SET " +
					"is_deleted = 1, updated_at = '2025-05-01 00:00:20' WHERE id = 3");
			}
			waitForDbreaderReplicationRowCount(dbReaderTable, 2);
			List<String> remainingIds = dstDBReader.readRows(
					"SELECT id FROM " + this.dstTablePrefix + dbReaderTable + " ORDER BY id");
			if (remainingIds.size() != 2 || !"1".equals(remainingIds.get(0)) || !"2".equals(remainingIds.get(1))) {
				throw new SyncLiteTestException("Phase 3: expected rows id=1,2 after soft-delete, got: " + remainingIds);
			}

			stopDBReaderJob();
			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(), e);
			try { stopDBReaderJob(); } catch (Exception ignored) {}
			postTest(testName, "FAIL");
		} finally {
			if (idempotencyToggled) {
				try {
					upsertConfigProperty(runtimeConsolidatorConfigPath, idempotentPropertyName, "false");
					restartSyncConsolidatorJob();
					waitForConsolidationStartup();
				} catch (Exception restoreEx) {
					globalTracer.error("Failed to restore consolidator idempotency setting after " + testName + " : " + restoreEx.getMessage(), restoreEx);
				}
			}
		}
	}

	private final void restartSyncConsolidatorJob() throws SyncLiteTestException {
		stopConsolidatorJob();
		waitForConsolidatorJobToStop();
		startSyncConsolidatorJob();
	}

	private final void upsertConfigProperty(Path configPath, String propertyName, String propertyValue) throws SyncLiteTestException {
		try {
			List<String> lines = Files.readAllLines(configPath);
			String normalizedPropertyName = propertyName.trim().toLowerCase();
			boolean updated = false;
			for (int i = 0; i < lines.size(); ++i) {
				String line = lines.get(i);
				String trimmed = line.trim();
				if (trimmed.isEmpty() || trimmed.startsWith("#")) {
					continue;
				}
				int sep = line.indexOf('=');
				if (sep <= 0) {
					continue;
				}
				String key = line.substring(0, sep).trim().toLowerCase();
				if (normalizedPropertyName.equals(key)) {
					lines.set(i, propertyName + " = " + propertyValue);
					updated = true;
					break;
				}
			}

			if (!updated) {
				lines.add(propertyName + " = " + propertyValue);
			}

			Files.write(configPath, lines);
		} catch (Exception e) {
			throw new SyncLiteTestException("Failed to update consolidator config property " + propertyName + " in file : " + configPath, e);
		}
	}

	private final void testSQLiteReinitializeDevice() throws SyncLiteTestException {		
		String testName = "testSQLiteReinitializeDevice";
		String tableName = testName;

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
					stmt.execute("CREATE TABLE " + tableName + "(col1 INTEGER PRIMARY KEY, col2 DOUBLE, col3 TEXT, col4 CLOB, col5 BLOB)");
					stmt.execute("INSERT INTO " + tableName + " VALUES(1, 1.1, '1', '1', '1')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(2, 2.2, '2', '2', '2')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(4, 4.4, '4', '4', '4')");
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5.5, '5', '5', '5')");

					stmt.execute("UPDATE " + tableName + " SET col1 = 3, col2 = 3.3, col3 = '3', col4 = '3', col5 = '3' WHERE col1 = 4");
					stmt.execute("DELETE FROM " + tableName + " WHERE col1 = 5");
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
			verifyDataWithDevice(testName, DeviceType.SQLITE,testDBPath, tableName, cols, orderCols);


			//Stop job			
			stopConsolidatorJob();

			//Reinitialize the device

			createManageDevicesConfigFile(testName);

			startManageDevicesConsolidatorJob();

			waitForConsolidatorJobToStop();

			startSyncConsolidatorJob();

			try (Connection conn = DriverManager.getConnection(testDBURL)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("INSERT INTO " + tableName + " VALUES(5, 5.5, '5', '5', '5')");
				}
			}

			waitForConsolidation(testName, DeviceType.SQLITE, testDBPath);

			verifyDataWithDevice(testName, DeviceType.SQLITE, testDBPath, tableName, cols, orderCols);

			SQLite.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}


	private final void waitForConsolidatorJobToStop() throws SyncLiteTestException {
		globalTracer.debug("Waiting for consolidator job to stop");
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
		globalTracer.debug("Creating manage-devices job configuration file");
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

	private final void testSQLiteStoreInsertWithColList() throws SyncLiteTestException {		
		String testName = "testSQLiteStoreInsertWithColList";
		String tabName = "testSQLiteStoreInsertWithColList";
		try {
			preTest(testName);

			Path testDBPath = dbDir.resolve(testName + ".db");			
			SQLiteStore.initialize(testDBPath, loggerConfig, testName);
			String testDBURL = "jdbc:synclite_sqlite_store:" + dbDir.resolve(testDBPath);

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
					stmt.execute("CREATE TABLE " + testName + "_dummy(col1 int)");
				}				
			}

			waitForConsolidation(testName, DeviceType.SQLITE_STORE, testDBPath);

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

			SQLiteStore.closeDevice(testDBPath);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(),  e);
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
					stmt.execute("CREATE TABLE " + testName + "_dummy(col1 int)");
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
			globalTracer.error("Details : " + e.getMessage(),  e);
			postTest(testName, "FAIL");
		}
	}

	private void testSQLiteInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testSQLiteInSyncLiteDB", DeviceType.SQLITE);
	}

	private void testDuckDBInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testDuckDBInSyncLiteDB", DeviceType.DUCKDB);
	}

	private void testH2InSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testH2InSyncLiteDB", DeviceType.H2);
	}

	private void testDerbyInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testDerbyInSyncLiteDB", DeviceType.DERBY);
	}

	private void testHyperSQLInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testHyperSQLInSyncLiteDB", DeviceType.HYPERSQL);
	}

	private void testSQLiteAppenderInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testSQLiteAppenderInSyncLiteDB", DeviceType.SQLITE_APPENDER);
	}

	private void testDuckDBAppenderInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testDuckDBAppenderInSyncLiteDB", DeviceType.DUCKDB_APPENDER);
	}

	private void testH2AppenderInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testH2AppenderInSyncLiteDB", DeviceType.H2_APPENDER);
	}

	private void testDerbyAppenderInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testDerbyAppenderInSyncLiteDB", DeviceType.DERBY_APPENDER);
	}

	private void testHyperSQLAppenderInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testHyperSQLAppenderInSyncLiteDB", DeviceType.HYPERSQL_APPENDER);
	}

	private void testStreamingInSyncLiteDB() throws SyncLiteTestException {
		testSyncLiteDB("testStreamingInSyncLiteDB", DeviceType.STREAMING);
	}

	private void testSyncLiteDB(String testName, DeviceType deviceType) throws SyncLiteTestException {
		Path testDBPath = dbDir.resolve(testName);
		try {			
			preTest(testName);
			//Initialize DB
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting initialize DB"); 
			globalTracer.debug("========================================================");
			SyncLiteDBResult r = initializeDB(testName, deviceType.toString(), testName, loggerConfig);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);

			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute initialize db operation : " + r.message);
			}
			globalTracer.debug("========================================================");


			//Start a transaction
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting begin transaction"); 
			globalTracer.debug("========================================================");
			r = beginTransaction(testName);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			globalTracer.debug("txn-handle: " + r.txnHandle);
			String txnHandle = r.txnHandle;
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute begin transaction operation : " + r.message);
			}
			globalTracer.debug("========================================================");

			//Create a Table
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting create table"); 
			globalTracer.debug("========================================================");
			r = executeSQL(testName, txnHandle, "create table "  + testName + "(a int, b varchar(50))", null);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute create table operation : " + r.message);
			}
			globalTracer.debug("========================================================");

			//Insert Data in a table
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting insert into table inside transaction"); 
			globalTracer.debug("========================================================");
			JSONArray arguments = new JSONArray();
			JSONArray rec1= new JSONArray();
			rec1.put(1);
			rec1.put("one");

			JSONArray rec2= new JSONArray();
			rec2.put(2);
			rec2.put("two");

			arguments.put(rec1);
			arguments.put(rec2);

			r = executeSQL(testName, txnHandle, "insert into " + testName + "(a,b) values(?, ?)", arguments);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute insert into table operation : " + r.message);
			}
			globalTracer.debug("========================================================");

			//Commit Transaction
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting commit transaction"); 
			globalTracer.debug("========================================================");
			r = commitTransaction(testName, txnHandle);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute commit transaction operation : " + r.message);
			}
			globalTracer.debug("========================================================");


			//Insert Data in a table
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting insert into table"); 
			globalTracer.debug("========================================================");
			arguments = new JSONArray();
			rec1= new JSONArray();
			rec1.put(3);
			rec1.put("three");

			rec2= new JSONArray();
			rec2.put(4);
			rec2.put("four");

			arguments.put(rec1);
			arguments.put(rec2);

			r = executeSQL(testName, null, "insert into " + testName + "(a,b) values(?, ?)", arguments);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute insert into table operation : " + r.message);
			}
			globalTracer.debug("========================================================");


			//Start a transaction
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting begin transaction"); 
			globalTracer.debug("========================================================");
			r = beginTransaction(testName);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			globalTracer.debug("txn-handle: " + r.txnHandle);
			txnHandle = r.txnHandle;
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute begin transaction operation : " + r.message);
			}
			globalTracer.debug("========================================================");

			//Insert Data in a table
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting insert into table inside transaction"); 
			globalTracer.debug("========================================================");
			arguments = new JSONArray();
			rec1= new JSONArray();
			rec1.put(5);
			rec1.put("five");

			arguments.put(rec1);

			r = executeSQL(testName, txnHandle, "insert into " + testName + "(a,b) values(?, ?)", arguments);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute insert into table operation : " + r.message);
			}
			globalTracer.debug("========================================================");

			//Rollback Transaction
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting rollback transaction"); 
			globalTracer.debug("========================================================");
			r = rollbackTransaction(testName, txnHandle);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute rollback transaction operation : " + r.message);
			}
			globalTracer.debug("========================================================");

			if (deviceType != DeviceType.STREAMING) {
				//Select from table
				globalTracer.debug("========================================================");
				globalTracer.debug("Excecuting select from table"); 
				globalTracer.debug("========================================================");
				r = executeSQL(testName, null, "select a, b from " + testName, null);
				globalTracer.debug("result : " + r.result);
				globalTracer.debug("message : " + r.message);

				if (r.result == false) {
					throw new SyncLiteTestException("Failed to execute select from table operation : " + r.message);
				}

				JSONArray resultSet = r.resultSet;

				if (resultSet.length() != 4) {
					throw new SyncLiteTestException("Expected resultset count : 4, received : " + resultSet.length() + " : " + resultSet.toString());
				}

				globalTracer.debug("========================================================");
			}
			
			//Now wait for consolidation and validate results
			waitForConsolidationOfSyncLiteDB(testName, deviceType, testDBPath);

			//Close DB
			globalTracer.debug("========================================================");
			globalTracer.debug("Excecuting close DB"); 
			globalTracer.debug("========================================================");
			r = closeDB(testName);
			globalTracer.debug("result : " + r.result);
			globalTracer.debug("message : " + r.message);
			globalTracer.debug("========================================================");

			if (r.result == false) {
				throw new SyncLiteTestException("Failed to execute close db operation : " + r.message);
			}


			List<String> cols = new ArrayList<String>();
			cols.add("a");
			cols.add("b");

			List<String> orderCols = new ArrayList<String>();
			orderCols.add("a");
			orderCols.add("b");

			List<String> expectedRows = new ArrayList<String>();
			expectedRows.add("1|one");
			expectedRows.add("2|two");
			expectedRows.add("3|three");
			expectedRows.add("4|four");

			verifyData(expectedRows, testName, cols, orderCols);

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(), e);
			postTest(testName, "FAIL");
		}
	}

	public static class SyncLiteDBResult {
		public boolean result;
		public String message;
		public JSONArray resultSet;
		public String txnHandle;
	}

	private static String syncLiteDBAddress = "http://localhost:5555";

	public JSONObject processRequest(JSONObject jsonRequest) throws SQLException {
		JSONObject jsonResponse = null;
		try {
			URL url = new URL(syncLiteDBAddress);

			HttpURLConnection conn = (HttpURLConnection) url.openConnection();

			// Set up connection properties
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json"); // Set content type as JSON
			conn.setDoOutput(true);
			conn.setConnectTimeout(1000000);
			conn.setReadTimeout(1000000);

			globalTracer.debug("Request JSON: " + jsonRequest.toString(4)); // Pretty print with 4 spaces

			// Send the JSON request
			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = jsonRequest.toString().getBytes("utf-8");
				os.write(input, 0, input.length);
			}
			

			// Get the response code
			int responseCode = conn.getResponseCode();
			globalTracer.debug("Response Code: " + responseCode);

			// If the response code is 200 OK, read the response
			if (responseCode == HttpURLConnection.HTTP_OK) {
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				String inputLine;
				StringBuilder response = new StringBuilder();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();

				// Parse the response JSON and print it
				jsonResponse = new JSONObject(response.toString());
				globalTracer.debug("Response JSON: " + jsonResponse.toString(4)); // Pretty print with 4 spaces

				// Access specific fields in the response JSON
				boolean result = jsonResponse.getBoolean("result");
				String message = jsonResponse.getString("message");

				globalTracer.debug("Result: " + result);
				globalTracer.debug("Message: " + message);
			} else {
				throw new SQLException("Failed to get a valid response from the server : " + responseCode);
			}	
		} catch (Exception e) {
			throw new SQLException("Failed to process request : " + e.getMessage(), e);
		}
		return jsonResponse;
	}

	public SyncLiteDBResult initializeDB(String dbName, String dbType, String deviceName, Path syncLiteLoggerConfigPath) throws SQLException{
		SyncLiteDBResult dbResult;
		try {
			JSONObject jsonRequest = new JSONObject();
			jsonRequest.put("db-type", dbType);
			jsonRequest.put("db-name", dbName);
			if (syncLiteLoggerConfigPath != null) {
				jsonRequest.put("synclite-logger-options", loadLoggerOptions(syncLiteLoggerConfigPath));
			}
			jsonRequest.put("sql", "initialize");

			JSONObject jsonRespose = processRequest(jsonRequest);

			dbResult = new SyncLiteDBResult();
			dbResult.result = jsonRespose.getBoolean("result");
			dbResult.message = jsonRespose.getString("message");
		} catch (Exception e) {
			throw new SQLException("Failed to initialize DB : " + dbName + " : " + e.getMessage(), e);
		}
		return dbResult;
	}

	public SyncLiteDBResult beginTransaction(String dbName) throws SQLException {
		SyncLiteDBResult dbResult;
		try {
			JSONObject jsonRequest = new JSONObject();
			jsonRequest.put("db-name", dbName);
			jsonRequest.put("sql", "begin");

			JSONObject jsonRespose = processRequest(jsonRequest);

			dbResult = new SyncLiteDBResult();
			dbResult.result = jsonRespose.getBoolean("result");
			dbResult.message = jsonRespose.getString("message");
			dbResult.txnHandle = jsonRespose.getString("txn-handle");
		} catch (Exception e) {
			throw new SQLException("Failed to begin transaction on DB : " + dbName + " : " + e.getMessage(), e);
		}
		return dbResult;
	}

	public SyncLiteDBResult commitTransaction(String dbName, String txnHandle) throws SQLException {
		SyncLiteDBResult dbResult;
		try {
			JSONObject jsonRequest = new JSONObject();
			jsonRequest.put("db-name", dbName);
			jsonRequest.put("txn-handle", txnHandle);
			jsonRequest.put("sql", "commit");

			JSONObject jsonRespose = processRequest(jsonRequest);

			dbResult = new SyncLiteDBResult();
			dbResult.result = jsonRespose.getBoolean("result");
			dbResult.message = jsonRespose.getString("message");
		} catch (Exception e) {
			throw new SQLException("Failed to commit transaction on DB : " + dbName + " : " + e.getMessage(), e);
		}
		return dbResult;
	}

	public SyncLiteDBResult rollbackTransaction(String dbName, String txnHandle) throws SQLException {
		SyncLiteDBResult dbResult;
		try {
			JSONObject jsonRequest = new JSONObject();
			jsonRequest.put("db-name", dbName);
			jsonRequest.put("sql", "rollback");
			jsonRequest.put("txn-handle", txnHandle);

			JSONObject jsonRespose = processRequest(jsonRequest);

			dbResult = new SyncLiteDBResult();
			dbResult.result = jsonRespose.getBoolean("result");
			dbResult.message = jsonRespose.getString("message");
		} catch (Exception e) {
			throw new SQLException("Failed to rollback transaction on DB : " + dbName + " : " + e.getMessage(), e);
		}
		return dbResult;
	}

	public SyncLiteDBResult executeSQL(String dbName, String txnHandle, String sql, JSONArray arguments) throws SQLException {
		SyncLiteDBResult dbResult;
		try {
			JSONObject jsonRequest = new JSONObject();
			jsonRequest.put("db-name", dbName);			
			jsonRequest.put("sql", sql);
			if (txnHandle != null) {
				jsonRequest.put("txn-handle", txnHandle);
			}
			if (arguments != null) {
				jsonRequest.put("arguments", arguments);
			}

			JSONObject jsonResponse = processRequest(jsonRequest);

			dbResult = new SyncLiteDBResult();
			dbResult.result = jsonResponse.getBoolean("result");
			dbResult.message = jsonResponse.getString("message");
			if (jsonResponse.has("resultset")) {
				dbResult.resultSet = jsonResponse.getJSONArray("resultset");
			}
		} catch (Exception e) {
			throw new SQLException("Failed to execute sql on DB : " + dbName + " : " + e.getMessage(), e);
		}
		return dbResult;
	}

	public SyncLiteDBResult closeDB(String dbName) throws SQLException {
		SyncLiteDBResult dbResult;
		try {
			JSONObject jsonRequest = new JSONObject();
			jsonRequest.put("db-name", dbName);
			jsonRequest.put("sql", "close");

			JSONObject jsonRespose = processRequest(jsonRequest);

			dbResult = new SyncLiteDBResult();
			dbResult.result = jsonRespose.getBoolean("result");
			dbResult.message = jsonRespose.getString("message");
		} catch (Exception e) {
			throw new SQLException("Failed to close DB : " + dbName + " : " + e.getMessage(), e);
		}
		return dbResult;
	}

	private JSONObject loadLoggerOptions(Path loggerConfigPath) throws IOException {
		JSONObject options = new JSONObject();
		List<String> lines = Files.readAllLines(loggerConfigPath, StandardCharsets.UTF_8);
		for (String line : lines) {
			String trimmed = line.trim();
			if (trimmed.isEmpty() || trimmed.startsWith("#")) {
				continue;
			}
			String[] kv = trimmed.split("=", 2);
			if (kv.length == 2 && !kv[0].trim().isEmpty()) {
				options.put(kv[0].trim(), kv[1].trim());
			}
		}
		return options;
	}

	private final void testQReader() throws SyncLiteTestException {
		String testName = "testQReader";

		if (!qreaderEnabled) {
			globalTracer.debug("Skipping " + testName + ": qreader not started");
			return;
		}

		Path qreaderTracePath = qreaderDbDir.resolve("synclite_qreader.trace");

		try {
			preTest(testName);

			// ── publish 3 test messages via MQTT ─────────────────────────────────
			// Topic format: <deviceName>/<topicName> — matches header-delimiter = /
			String mqttTopic = QREADER_DEVICE_NAME + "/" + QREADER_TABLE;
			String clientId = "synclite-validator-" + UUID.randomUUID();
			try (MqttClient mqttClient = new MqttClient(MQTT_BROKER_URL, clientId, new MemoryPersistence())) {
				MqttConnectOptions opts = new MqttConnectOptions();
				opts.setCleanSession(true);
				opts.setConnectionTimeout(10);
				mqttClient.connect(opts);
				for (int i = 1; i <= 3; i++) {
					String payload = "val" + i + "_col1,val" + i + "_col2";
					MqttMessage msg = new MqttMessage(payload.getBytes());
					msg.setQos(1);
					mqttClient.publish(mqttTopic, msg);
					globalTracer.debug("Published MQTT message: " + payload + " to topic: " + mqttTopic);
				}
				mqttClient.disconnect();
			}

			// ── wait for all 3 rows to appear in the consolidated destination ───
			Thread.sleep(2000);
			long qreaderCommitId = 0;
			if (Files.exists(qreaderDeviceDbPath)) {
				try (Connection qConn = DriverManager.getConnection("jdbc:sqlite:" + qreaderDeviceDbPath);
						Statement qStmt = qConn.createStatement();
						ResultSet qRs = qStmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
					if (qRs.next()) {
						qreaderCommitId = qRs.getLong(1);
					}
				} catch (Exception qe) {
					globalTracer.error("Failed to read qreader device commit id", qe);
				}
			}

			if (qreaderCommitId == 0) {
				String qTraceTail = "";
				if (Files.exists(qreaderTracePath)) {
					try {
						List<String> traceLines = Files.readAllLines(qreaderTracePath);
						int from = Math.max(0, traceLines.size() - 20);
						qTraceTail = String.join("\n", traceLines.subList(from, traceLines.size()));
					} catch (Exception ignored) {}
				}
				throw new SyncLiteTestException("qreader did not generate any device transaction (commit_id=0). qreader trace tail: " + qTraceTail);
			}

			waitForDbreaderReplicationRowCount(QREADER_TABLE, 3);

			// ── spot-check the first message's col1 value ────────────────────────
			List<String> col1Values = dstDBReader.readRows(
				"SELECT col1 FROM " + this.dstTablePrefix + QREADER_TABLE + " ORDER BY col1");
			if (col1Values.isEmpty() || !col1Values.contains("val1_col1")) {
				throw new SyncLiteTestException("testQReader: expected 'val1_col1' in col1 results, got: " + col1Values);
			}

			postTest(testName, "PASS");
		} catch (Exception e) {
			globalTracer.error("Failed Test : " + testName);
			globalTracer.error("Details : " + e.getMessage(), e);
			postTest(testName, "FAIL");
		}
	}

}
