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

<%@page import="java.time.ZoneId"%>
<%@page import="java.time.LocalDateTime"%>
<%@page import="java.time.ZonedDateTime"%>
<%@page import="java.time.Instant"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.InputStreamReader"%>
<%@page import="javax.websocket.Session"%>
<%@page import="java.nio.file.Files"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>SyncLite Validator Dashboard</title>
<script type="text/javascript">

function autoRefreshSetTimeout() {
    const refreshInterval = parseInt(document.getElementById("refresh-interval").value);
    
    if (!isNaN(refreshInterval)) {
    	const val = refreshInterval * 1000;
    	if (val === 0) {
    		const timeoutObj = setTimeout("autoRefresh()", 1000);
    		clearTimeout(timeoutObj);    		
    	} else {    		
    		setTimeout("autoRefresh()", val);
    	}
	}	
}

function autoRefresh() {
	document.forms['dashboardForm'].submit();
}
</script>
</head>

<body onload="autoRefreshSetTimeout()">	
	<%@include file="html/menu.html"%>	
	<div class="main">
		<h2>SyncLite Validator Dashboard</h2>
		<%
			if ((session.getAttribute("job-status") == null) || (session.getAttribute("test-root") == null)) {
				out.println("<h4 style=\"color: red;\"> Please configure and start the validator job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();		
			}		
		
		
			Path validatorDBPath = Path.of(session.getAttribute("test-root").toString(), "workDir", "synclite_validator.db");		
			if (!Files.exists(validatorDBPath)) {
				out.println("<h4 style=\"color: red;\"> Awaiting statistics file to get created for validator job. Please click on Test Dashboard again.</h4>");
				throw new javax.servlet.jsp.SkipPageException();				
			}		
		%>

		<center>
			<%-- response.setIntHeader("Refresh", 2); --%>
			<table>
				<tbody>
					<%
					int refreshInterval = 5;
								if (request.getParameter("refresh-interval") != null) {
									try {
										refreshInterval = Integer.valueOf(request.getParameter("refresh-interval").toString());
									} catch (Exception e) {
										refreshInterval = 5;
									}
								}

								//Get current job PID if running
								int testCnt = 0;
								int passedTestCnt = 0;
								int failedTestCnt = 0;
								long currentConsolidatorJobPID = 0;
								{
									Process jpsProc = Runtime.getRuntime().exec("jps -l -m");
									BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
									String line = stdout.readLine();
									while (line != null) {
										if (line.contains("com.synclite.consolidator.Main")) {
											currentConsolidatorJobPID = Long.valueOf(line.split(" ")[0]);
										}
										line = stdout.readLine();
									}
								}

								//Get current job PID if running
								long currentSyncLiteDBPID = 0;
								{
									Process jpsProc = Runtime.getRuntime().exec("jps -l -m");
									BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
									String line = stdout.readLine();
									while (line != null) {
										if (line.contains("com.synclite.db.Main")) {
											currentSyncLiteDBPID = Long.valueOf(line.split(" ")[0]);
										}
										line = stdout.readLine();
									}
								}

								long currentValidatorJobPID = 0;
								{
									Process jpsProc = Runtime.getRuntime().exec("jps -l -m");
									BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
									String line = stdout.readLine();
									while (line != null) {
										if (line.contains("com.synclite.validator.Main")) {
											currentValidatorJobPID = Long.valueOf(line.split(" ")[0]);
										}
										line = stdout.readLine();
									}			
								}
								
								long elapsedTime = 0;
								if (request.getSession().getAttribute("job-status") != null) {
									if (request.getSession().getAttribute("job-status").toString().equals("STARTED")) {
										if (request.getSession().getAttribute("job-start-time") != null) {
											long startTime = Long
													.valueOf(request.getSession().getAttribute("job-start-time").toString());
											long currentTime = System.currentTimeMillis();

											elapsedTime = (currentTime - startTime) / 1000;
										}
									}
								}

								out.println("<table>");

								out.println("<tr>");
								out.println("<td> Consolidator Job Process ID </td>");
								out.println("<td>" + currentConsolidatorJobPID + "</td>");
								out.println("</tr>");
								out.println("<tr>");
								out.println("<td> SyncLite DB Process ID </td>");
								out.println("<td>" + currentSyncLiteDBPID + "</td>");
								out.println("</tr>");
								out.println("<tr>");
								out.println("<td> Validator Job Process ID </td>");
								out.println("<td>" + currentValidatorJobPID + "</td>");
								out.println("</tr>");
								out.println("<tr>");
								out.println("<td> Elapsed Time </td>");
								String elapsedTimeStr = "";
								long elapsedTimeDays = 0L;
								long elapsedTimeHours = 0L;
								long elapsedTimeMinutes = 0L;
								long elapsedTimeSeconds = 0L;

								if (elapsedTime > 86400) {
									elapsedTimeDays = elapsedTime / 86400;
									if (elapsedTimeDays > 0) {
										if (elapsedTimeDays == 1) {
											elapsedTimeStr = elapsedTimeStr + elapsedTimeDays + " Day ";
										} else {
											elapsedTimeStr = elapsedTimeStr + elapsedTimeDays + " Days ";
										}
									}
								}

								if (elapsedTime > 3600) {
									elapsedTimeHours = (elapsedTime % 86400) / 3600;
									if (elapsedTimeHours > 0) {
										if (elapsedTimeHours == 1) {
											elapsedTimeStr = elapsedTimeStr + elapsedTimeHours + " Hour ";
										} else {
											elapsedTimeStr = elapsedTimeStr + elapsedTimeHours + " Hours ";
										}
									}
								}

								if (elapsedTime > 60) {
									elapsedTimeMinutes = (elapsedTime % 3600) / 60;
									if (elapsedTimeMinutes > 0) {
										if (elapsedTimeMinutes == 1) {
											elapsedTimeStr = elapsedTimeStr + elapsedTimeMinutes + " Minute ";
										} else {
											elapsedTimeStr = elapsedTimeStr + elapsedTimeMinutes + " Minutes ";
										}

									}
								}
								elapsedTimeSeconds = elapsedTime % 60;
								if (elapsedTimeSeconds == 1) {
									elapsedTimeStr = elapsedTimeStr + elapsedTimeSeconds + " Second";
								} else {
									elapsedTimeStr = elapsedTimeStr + elapsedTimeSeconds + " Seconds";
								}
							
								out.println("<td>" + elapsedTimeStr + "</td>");
								out.println("</tr>");

								out.println("<tr>");
								out.println("<td></td>");
								out.println("<td>");
								out.println("<form name=\"dashboardForm\" method=\"post\" action=\"dashboard.jsp\">");
								out.println("<div class=\"pagination\">");
								out.println("REFRESH IN ");
								out.println("<input type=\"text\" id=\"refresh-interval\" name=\"refresh-interval\" value =\""
										+ refreshInterval + "\" size=\"1\" onchange=\"autoRefreshSetTimeout()\">");
								out.println(" SECONDS");
								out.println("</div>");
								out.println("</form>");
								out.println("</td>");
								out.println("</tr>");

								out.println("<tr>");
								out.println("</tr>");
								out.println("</table>");

								Class.forName("org.sqlite.JDBC");

								try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + validatorDBPath)) {
									try (Statement stat = conn.createStatement()) {
										ResultSet rs = stat.executeQuery(
												"SELECT test_name, start_time, end_time, execution_time, status FROM test_results");
										out.println("<table>");
										out.println("<tbody>");
										out.println("<tr>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<th>");
										out.println("Test Name");
										out.println("</th>");
										out.println("<th>");
										out.println("Start Time");
										out.println("</th>");
										out.println("<th>");
										out.println("End Time");
										out.println("</th>");
										out.println("<th>");
										out.println("Duration (ms)");
										out.println("</th>");
										out.println("<th>");
										out.println("Status");
										out.println("</th>");
										out.println("</tr>");
										while (rs.next()) {
											out.println("<tr>");
											out.println("<td>" + rs.getString("test_name") + "</td>");
											out.println("<td>" + rs.getString("start_time") + "</td>");
											String endTime = rs.getString("end_time");
											if (endTime == null) {
												endTime = "";
											}
											out.println("<td>" + endTime + "</td>");
											out.println("<td>" + rs.getString("execution_time") + "</td>");
											out.println("<td>" + rs.getString("status") + "</td>");
											out.println("</tr>");
											++testCnt;
											String status = rs.getString("status");
											if (status != null) {
												if (status.equals("PASS")) {
													++passedTestCnt;
												}
												if (status.equals("FAIL")) {
													++failedTestCnt;
												}
											}
										}
									}
								}
					%>
				</tbody>
			</table>
			<table>
				<tr>
					<td>Total <%= testCnt%>
					</td>
					<td>Passed <%= passedTestCnt%>
					</td>
					<td>Failed <%= failedTestCnt%>
					</td>
				</tr>
			</table>
		</center>
	</div>
</body>
</html>