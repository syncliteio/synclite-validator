<%-- 
    Copyright (c) 2024 mahendra.chavan@syncLite.io, all rights reserved.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
    in compliance with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
    or implied.  See the License for the specific language governing permissions and limitations
    under the License.
--%>

<%@page import="java.nio.file.Files"%>
<%@page import="java.time.Instant"%>
<%@page import="java.io.File"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.List"%>
<%@page import="java.util.ArrayList"%>
<%@page import="javax.websocket.Session"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<script type="text/javascript">
</script>
<title>Configure Test Job</title>
</head>
<%
String errorMsg = request.getParameter("errorMsg");

Path testRoot, dbPath,stageDirPath, workDirPath;	

if (request.getAttribute("test-root") != null) {
	testRoot = Path.of(request.getAttribute("test-root").toString());
	dbPath = Path.of(testRoot.toString(), "db");
	stageDirPath = Path.of(testRoot.toString(), "stageDir");
	workDirPath = Path.of(testRoot.toString(), "workDir");	
} else {
	testRoot = Path.of(System.getProperty("user.home"), "synclite", "test");
	dbPath = Path.of(System.getProperty("user.home"), "synclite", "test", "db");
	stageDirPath = Path.of(System.getProperty("user.home"), "synclite", "test", "stageDir");
	workDirPath = Path.of(System.getProperty("user.home"), "synclite", "test", "workDir");
}

Integer numThreads = 0;
if (request.getAttribute("test-num-threads") != null) {
	numThreads = Integer.valueOf(request.getAttribute("test-num-threads").toString());
} else {
	numThreads = Runtime.getRuntime().availableProcessors();
}

Path loggerConfig;
if (request.getAttribute("logger-config") != null) {
	loggerConfig = Path.of(request.getAttribute("logger-config").toString()); 
} else {	
	loggerConfig = Path.of(getServletContext().getRealPath("/WEB-INF/lib"), "config", "logger", "default");
}

Path consolidatorConfig;
if (request.getAttribute("consolidator-config") != null) {
	consolidatorConfig = Path.of(request.getAttribute("consolidator-config").toString()); 
} else {
	consolidatorConfig = Path.of(getServletContext().getRealPath("/WEB-INF/lib"), "config", "consolidator", "DST-SQLITE_STAGE-FS_MODE-CONSOLIDATION.conf");
}


String executionMode;
if (request.getAttribute("execution-mode") != null) {
	executionMode = request.getAttribute("execution-mode").toString(); 
} else {
	executionMode = "EXECUTE";
}


String[] tests;
if (request.getParameterValues("tests") != null) {
	tests = request.getParameterValues("tests");
} else {
	tests = new String[1];
	tests[0] = "all";
}

//Load logger-configs from config dir
Path loggerConfigPath = Path.of(getServletContext().getRealPath("/WEB-INF/lib"), "config", "logger");
File[] loggerConfigs = new File[1];
File loggerConfigDir = loggerConfigPath.toFile();
if (loggerConfigDir.isDirectory()) {
	loggerConfigs = loggerConfigDir.listFiles();
}

//Load consolidator-configs from config dir
Path consolidatorConfigPath = Path.of(getServletContext().getRealPath("/WEB-INF/lib"), "config", "consolidator");
File[] consolidatorConfigs = new File[1];
File consolidatorConfigDir = consolidatorConfigPath.toFile();
if (consolidatorConfigDir.isDirectory()) {
	consolidatorConfigs = consolidatorConfigDir.listFiles();
}

/*
//Load test list from test.conf
Path testConfigPath = Path.of("config", "test");
BufferedReader reader = null;
List<String> allTests = new ArrayList<String>();

try {
	if (Files.exists(testConfigPath)) {		
   	    reader = new BufferedReader(new FileReader(testConfigPath.toFile()));
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
			allTests.add(line.trim());
			line = reader.readLine();
		}
		reader.close();
	}
} catch (Exception e){ 
	if (reader != null) {
		reader.close();
	}
	throw e;
} 
*/
%>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Configure SyncLite Validator</h2>
		<%
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateJobConfiguration"
			method="post">

			<table>
				<tbody>
					<tr>
						<td>Test Directory</td>
						<td><input type="text" size=30 id="test-root"
							name="test-root"
							value="<%=testRoot%>" readonly
							title="Specify a work directory for SyncLite Validator."/>
						</td>
					</tr>

					<tr>
						<td>Number of Threads</td>
						<td><input type="number" size=30 id="test-num-threads"
							name="test-num-threads"
							value="<%=numThreads%>" readonly
							title="Specify number of threads to use for running tests."/>
						</td>
					</tr>
					
					<tr>
						<td>Logger Configuration</td>
						<td><select id="logger-config" name="logger-config" title="Select logger configuration to use for SyncLite devices.">
								<%
									for (File c : loggerConfigs) {
										String cName = c.getAbsolutePath();
										if (cName.equals(loggerConfig.toString())) {
											out.println("<option value=\"" + cName + "\" selected>" + c.getName() + "</option>");
										} else {
											out.println("<option value=\"" + cName + "\">" + c.getName() + "</option>");
										}
									}
								%>
						</select></td>
					</tr>
					<tr>
						<td>Consolidator Configuration</td>
						<td><select id="consolidator-config" name="consolidator-config" title="Select consolidator configuration.">
								<%
									for (File c : consolidatorConfigs) {
										String cName = c.getAbsolutePath();
										if (cName.equals(consolidatorConfig.toString())) {
											out.println("<option value=\"" + cName + "\" selected>" + c.getName() + "</option>");
										} else {
											out.println("<option value=\"" + cName + "\">" + c.getName() + "</option>");
										}
									}
								%>
						</select></td>
					</tr>
					
					<tr>
						<td>Execution Mode</td>
						<td><select id="execution-mode" name="execution-mode" title="Select execution mode.">
								<%
									if (executionMode.equals("EXECUTE")) {
										out.println("<option value=\"EXECUTE\" selected>" + "EXECUTE" + "</option>");
									} else {
										out.println("<option value=\"EXECUTE\">" + "EXECUTE" + "</option>");
									}
									if (executionMode.equals("DEBUG")) {
										out.println("<option value=\"DEBUG\" selected>" + "DEBUG" + "</option>");
									} else {
										out.println("<option value=\"DEBUG\">" + "DEBUG" + "</option>");
									}
								%>
						</select></td>
					</tr>
					
				</tbody>				
			</table>
			<center>
				<button type="submit" name="next">Run</button>
			</center>			
		</form>
	</div>
</body>
</html>