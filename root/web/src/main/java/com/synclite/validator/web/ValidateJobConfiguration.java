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

package com.synclite.validator.web;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateJobConfiguration")
public class ValidateJobConfiguration extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateJobConfiguration() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {
			doGet(request, response);

			String testRoot = request.getParameter("test-root");

			Path testRootPath, stageDirPath, commandDirPath, dbDirPath, workDirPath;
			if ((testRoot == null) || testRoot.trim().isEmpty()) {
				throw new ServletException("\"Test Root\" must be specified");
			} else {
				testRootPath = Path.of(testRoot);
				if (! Files.exists(testRootPath)) {
					throw new ServletException("Specified \"Test Root\" : " + testRoot + " does not exist, please specify a valid \"Data Directory\"");
				} else {
					if (! testRootPath.toFile().canRead()) {
						throw new ServletException("Specified \"Test Directory\" does not have read permission");
					}

					if (! testRootPath.toFile().canWrite()) {
						throw new ServletException("Specified \"Test Directory\" does not have write permission");
					}

					//Delete existing directory if present and recreate	
					if (Files.exists(testRootPath)) {
						// Delete all the files and subdirectories in the directory
						Files.walk(testRootPath)
						.sorted((p1, p2) -> -p1.compareTo(p2))
						.forEach(path -> {
							try {
								Files.delete(path);
							} catch (Exception e) {
								e.printStackTrace();
							}
						});
					}

					//Create db, workDir and testDir if not present 
					stageDirPath = Path.of(testRootPath.toString(), "stageDir");
					commandDirPath = Path.of(testRootPath.toString(), "commandDir");
					workDirPath = Path.of(testRootPath.toString(),"workDir");
					dbDirPath = Path.of(testRootPath.toString(),"db");
					//Delete existing directory if present and recreate	

					Files.createDirectories(dbDirPath);
					Files.createDirectories(stageDirPath);
					Files.createDirectories(commandDirPath);
					Files.createDirectories(workDirPath);
				} 
			}

			String testNumThreadsStr = request.getParameter("test-num-threads");
			try {
				int numThreads = Integer.valueOf(testNumThreadsStr);
				if (numThreads <= 0) {
					throw new ServletException("\"Number of Threads\" must be a valid positive numeric value");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("\"Number of Threads\" must be a valid positive numeric value");
			}


			String loggerConfig = request.getParameter("logger-config");
			Path loggerConfigPath;
			if ((loggerConfig == null) || loggerConfig.trim().isEmpty()) {
				throw new ServletException("\"SyncLite Logger Config File\" must be specified");
			} else {
				loggerConfigPath = Path.of(loggerConfig);
				if (! Files.exists(loggerConfigPath)) {
					throw new ServletException("Specified \"SyncLite Logger Config File\" : " + loggerConfig + " does not exist, please specify a valid \"SyncLite Logger Config File\"");
				}
			}

			String consolidatorConfig = request.getParameter("consolidator-config");
			Path consolidatorConfigPath;
			if ((consolidatorConfig == null) || consolidatorConfig.trim().isEmpty()) {
				throw new ServletException("\"SyncLite Consolidator Config File\" must be specified");
			} else {
				consolidatorConfigPath = Path.of(consolidatorConfig);
				if (! Files.exists(consolidatorConfigPath)) {
					throw new ServletException("Specified \"SyncLite Consolidator Config File\" : " + consolidatorConfig + " does not exist, please specify a valid \"SyncLite Consolidator Config File\"");
				}
			}

			String executionMode = request.getParameter("execution-mode");

			//Copy the consolidator config file to workDir

			Path consolidatorConfigPathInWorkDir = workDirPath.resolve("synclite_consolidator.conf");
			Files.copy(consolidatorConfigPath, consolidatorConfigPathInWorkDir);

			//Replace respective paths in config files
			//Replace local-stage-directory in logger config
			//Replace device-upload-root in consolidator config
			//Replace lincense-file in consolidator config

			HashMap<String, String> confToReplace = new HashMap<String, String>();
			confToReplace.put("local-data-stage-directory", stageDirPath.toString());
			confToReplace.put("local-command-stage-directory", commandDirPath.toString());
			replaceConfigValue(loggerConfigPath, confToReplace);


			confToReplace.clear(); 
			confToReplace.put("device-upload-root", stageDirPath.toString());	
			confToReplace.put("device-command-root", commandDirPath.toString());
			confToReplace.put("device-data-root", workDirPath.toString());
			confToReplace.put("dst-telemetry-root-1", workDirPath.toString());

			replaceConfigValue(consolidatorConfigPathInWorkDir, confToReplace);

			//
			//Start consolidator job by passing consolidatorConfig
			//

			String corePath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib").toString();

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
				//Kill job if found
				if(currentJobPID > 0) {
					if (isWindows()) {
						Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
					} else {
						Runtime.getRuntime().exec("kill -9 " + currentJobPID);
					}
				}

				//Start job again
				Process p;
				if (isWindows()) {
					String scriptName = "synclite-consolidator.bat";
					String scriptPath = Path.of(corePath, scriptName).toString();
					//String cmd = "\"" + scriptPath + "\"" + " sync " + " --work-dir " + "\"" + workDirPath + "\"" + " --config " + "\"" + consolidatorConfigPathInWorkDir + "\"";
					String[] cmdArray = {scriptPath.toString(), "sync", "--work-dir", workDirPath.toString() ,"--config", consolidatorConfigPathInWorkDir.toString()};
					p = Runtime.getRuntime().exec(cmdArray);
				} else {				
					String scriptName = "synclite-consolidator.sh";
					Path scriptPath = Path.of(corePath, scriptName);

					//First add execute permission				
					/*
					String [] command = {"/bin/chmod","+x", scriptPath};
					Runtime rt = Runtime.getRuntime();
					Process pr = rt.exec( command );
					pr.waitFor();
					 */				

					// Get the current set of script permissions
					Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
					// Add the execute permission if it is not already set
					if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
						perms.add(PosixFilePermission.OWNER_EXECUTE);
						Files.setPosixFilePermissions(scriptPath, perms);
					}

					String[] cmdArray = {scriptPath.toString(), "sync", "--work-dir", workDirPath.toString() ,"--config", consolidatorConfigPathInWorkDir.toString()};
					p = Runtime.getRuntime().exec(cmdArray);
				}

				//int exitCode = p.exitValue();
				//Thread.sleep(3000);
				Thread.sleep(5000);
				boolean processStatus = p.isAlive();
				if (!processStatus) {
					BufferedReader procErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
					if ((line = procErr.readLine()) != null) {
						StringBuilder errorMsg = new StringBuilder();
						int i = 0;
						do {
							errorMsg.append(line);
							errorMsg.append("\n");
							line = procErr.readLine();
							if (line == null) {
								break;
							}
							++i;
						} while (i < 5);
						throw new ServletException("Failed to start consolidator job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
					}

					BufferedReader procOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
					if ((line = procOut.readLine()) != null) {
						StringBuilder errorMsg = new StringBuilder();
						int i = 0;
						do {
							errorMsg.append(line);
							errorMsg.append("\n");
							line = procOut.readLine();
							if (line == null) {
								break;
							}
							++i;
						} while (i < 5);
						throw new ServletException("Failed to start consolidator job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
					}
					throw new ServletException("Failed to start consolidator job with exit value : " + p.exitValue());
				}
			}
			
			
			Path syncLiteDBConfPath = Path.of(corePath, "synclite_db.conf");
			confToReplace = new HashMap<String, String>();
			confToReplace.put("trace-level", "DEBUG");
			confToReplace.put("num-threads", "8");
			addConfigValue(syncLiteDBConfPath, confToReplace);

			//
			//Start synclite-db job with default settings
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
				//Kill job if found
				if(currentJobPID > 0) {
					if (isWindows()) {
						Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
					} else {
						Runtime.getRuntime().exec("kill -9 " + currentJobPID);
					}
				}

				//Start job again
				Process p;
				if (isWindows()) {
					String scriptName = "synclite-db.bat";
					String scriptPath = Path.of(corePath, scriptName).toString();
					//String cmd = "\"" + scriptPath + "\"" + " sync " + " --work-dir " + "\"" + workDirPath + "\"" + " --config " + "\"" + consolidatorConfigPathInWorkDir + "\"";
					String[] cmdArray = {scriptPath.toString(), "--config", syncLiteDBConfPath.toString()};
					p = Runtime.getRuntime().exec(cmdArray);
				} else {				
					String scriptName = "synclite-db.sh";
					Path scriptPath = Path.of(corePath, scriptName);

					//First add execute permission				
					/*
					String [] command = {"/bin/chmod","+x", scriptPath};
					Runtime rt = Runtime.getRuntime();
					Process pr = rt.exec( command );
					pr.waitFor();
					 */

					// Get the current set of script permissions
					Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
					// Add the execute permission if it is not already set
					if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
						perms.add(PosixFilePermission.OWNER_EXECUTE);
						Files.setPosixFilePermissions(scriptPath, perms);
					}

					String[] cmdArray = {scriptPath.toString(), "--config", syncLiteDBConfPath.toString()};
					p = Runtime.getRuntime().exec(cmdArray);
				}

				//int exitCode = p.exitValue();
				//Thread.sleep(3000);
				Thread.sleep(5000);
				boolean processStatus = p.isAlive();
				if (!processStatus) {
					BufferedReader procErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
					if ((line = procErr.readLine()) != null) {
						StringBuilder errorMsg = new StringBuilder();
						int i = 0;
						do {
							errorMsg.append(line);
							errorMsg.append("\n");
							line = procErr.readLine();
							if (line == null) {
								break;
							}
							++i;
						} while (i < 5);
						throw new ServletException("Failed to start synclite-db with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
					}

					BufferedReader procOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
					if ((line = procOut.readLine()) != null) {
						StringBuilder errorMsg = new StringBuilder();
						int i = 0;
						do {
							errorMsg.append(line);
							errorMsg.append("\n");
							line = procOut.readLine();
							if (line == null) {
								break;
							}
							++i;
						} while (i < 5);
						throw new ServletException("Failed to start synclite-db with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
					}
					throw new ServletException("Failed to start synclite-db with exit value : " + p.exitValue());
				}
			}
			
			
			//
			//Start validator core process by passing testRoot and loggerConfig
			//
			//Get current job PID if running
			//
			//If execution mode is DEBUG then just print the commandline with arguments for the developer
			//to start validator process in debugger
			//

			String debugStr = "";		
			{
				long currentJobPID = 0;
				Process jpsProc = Runtime.getRuntime().exec("jps -l -m");
				BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
				String line = stdout.readLine();
				while (line != null) {
					if (line.contains("com.synclite.validator.Main")) {
						currentJobPID = Long.valueOf(line.split(" ")[0]);
					}
					line = stdout.readLine();
				}
				//Kill job if found
				if(currentJobPID > 0) {
					if (isWindows()) {
						Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
					} else {
						Runtime.getRuntime().exec("kill -9 " + currentJobPID);
					}
				}

				//Start job again
				Process p = null;
				if (isWindows()) {
					String scriptName = "synclite-validator.bat";
					String scriptPath = Path.of(corePath, scriptName).toString();
					//String cmd = "\"" + scriptPath + "\"" + " test --test-root " + "\"" + testRootPath +  "\"" + " --logger-config " + "\"" + loggerConfigPath + "\"" + " --consolidator-config " + "\"" + consolidatorConfigPathInWorkDir + "\"";
					String[] cmdArray = {scriptPath.toString(), "test", "--test-root", testRootPath.toString(), "--logger-config", loggerConfigPath.toString(), "--consolidator-config", consolidatorConfigPathInWorkDir.toString(), "--core-path", corePath.toString(), "--num-threads", testNumThreadsStr};
					if (executionMode.equals("EXECUTE")) {
						p = Runtime.getRuntime().exec(cmdArray);
					} else {						
						debugStr = "test" + " --test-root " + "\"" + testRootPath.toString() + "\"" + " --logger-config " + "\"" + loggerConfigPath.toString() + "\"" + " --consolidator-config " + "\"" + consolidatorConfigPathInWorkDir + "\""  + " --core-path " + "\"" + corePath + "\"" + " --num-threads " + testNumThreadsStr; 
					}
				} else {				
					String scriptName = "synclite-validator.sh";
					Path scriptPath = Path.of(corePath, scriptName);

					//First add execute permission				
					/*
					String [] command = {"/bin/chmod","+x", scriptPath};
					Runtime rt = Runtime.getRuntime();
					Process pr = rt.exec( command );
					pr.waitFor();				
					 */

					// Get the current set of script permissions
					Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
					// Add the execute permission if it is not already set
					if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
						perms.add(PosixFilePermission.OWNER_EXECUTE);
						Files.setPosixFilePermissions(scriptPath, perms);
					}

					String[] cmdArray = {scriptPath.toString(), "test", "--test-root", testRootPath.toString(), "--logger-config", loggerConfigPath.toString(), "--consolidator-config", consolidatorConfigPathInWorkDir.toString(), "--core-path", corePath.toString(), "--num-threads", testNumThreadsStr};

					if (executionMode.equals("EXECUTE")) {
						p = Runtime.getRuntime().exec(cmdArray);
					} else {
						debugStr = "test" + " --test-root " + "\"" + testRootPath.toString() + "\"" + " --logger-config " + "\"" + loggerConfigPath.toString() + "\"" + " --consolidator-config " + "\"" + consolidatorConfigPathInWorkDir + "\""  + " --core-path " + "\"" + corePath + "\"" + " --num-threads " + testNumThreadsStr ;
					}
				}

				if (executionMode.equals("EXECUTE")) {
					Thread.sleep(1000);
					boolean processStatus = p.isAlive();
					if (!processStatus) {
						BufferedReader procErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
						if ((line = procErr.readLine()) != null) {
							StringBuilder errorMsg = new StringBuilder();
							int i = 0;
							do {
								errorMsg.append(line);
								errorMsg.append("\n");
								line = procErr.readLine();
								if (line == null) {
									break;
								}
								++i;
							} while (i < 5);
							throw new ServletException("Failed to start validator job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
						}

						BufferedReader procOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
						if ((line = procOut.readLine()) != null) {
							StringBuilder errorMsg = new StringBuilder();
							int i = 0;
							do {
								errorMsg.append(line);
								errorMsg.append("\n");
								line = procOut.readLine();
								if (line == null) {
									break;
								}
								++i;
							} while (i < 5);
							throw new ServletException("Failed to start validator job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
						}
						throw new ServletException("Failed to start validator job with exit value : " + p.exitValue());
					}
				}
			}


			request.getSession().setAttribute("test-root",testRoot); 
			request.getSession().setAttribute("test-num-threads", testNumThreadsStr); 
			request.getSession().setAttribute("logger-config",loggerConfig);
			request.getSession().setAttribute("consolidator-config",consolidatorConfigPathInWorkDir);
			request.getSession().setAttribute("job-status","STARTED");
			request.getSession().setAttribute("job-start-time",System.currentTimeMillis());
			request.getSession().setAttribute("execution-mode", executionMode);
			if (executionMode.equals("EXECUTE")) {
				response.sendRedirect("dashboard.jsp");
			} else {
				throw new ServletException("Debug with arguments " + debugStr);
			}
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureTestJob.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private void replaceConfigValue(Path configPath, HashMap<String, String> confToReplace) throws ServletException {
		Path propsPath = configPath;
		HashMap<String, String> properties = new HashMap<String, String>();

		BufferedReader reader = null;
		try {
			if (Files.exists(propsPath)) {
				reader = new BufferedReader(new FileReader(propsPath.toFile()));
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
					if (confToReplace.containsKey(propName)) {
						properties.put(propName, confToReplace.get(propName));
					} else {
						properties.put(propName, propValue);
					}
					line = reader.readLine();
				}
				reader.close();
			}

			//Write out contents of properties into the supplied config file.
			StringBuilder propStr = new StringBuilder();
			for (Map.Entry<String, String> entry : properties.entrySet()) {
				propStr.append(entry.getKey());
				propStr.append("=");
				propStr.append(entry.getValue());
				propStr.append("\n");
			}

			Files.writeString(configPath, propStr.toString(), StandardOpenOption.TRUNCATE_EXISTING);

		} catch (Exception e){ 
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
					throw new ServletException("Failed to close config file : " + e1.getMessage(), e1);
				}
			}
			throw new ServletException("Failed to replace config value in config file : " + e.getMessage() ,e);
		}	
	}

	private void addConfigValue(Path configPath, HashMap<String, String> confToAdd) throws ServletException {
		try {
			//Write out contents of properties into the supplied config file.
			StringBuilder propStr = new StringBuilder();
			for (Map.Entry<String, String> entry : confToAdd.entrySet()) {
				propStr.append(entry.getKey());
				propStr.append("=");
				propStr.append(entry.getValue());
				propStr.append("\n");
			}

			Files.writeString(configPath, propStr.toString(), StandardOpenOption.TRUNCATE_EXISTING);

		} catch (Exception e){ 
			throw new ServletException("Failed to add config value in config file : " + e.getMessage() ,e);
		}	
	}

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}

}


