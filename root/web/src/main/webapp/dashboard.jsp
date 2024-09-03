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
</head>

<body>	
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
				out.println("<h4 style=\"color: red;\"> Statistics file for the consolidator job is missing .</h4>");
				throw new javax.servlet.jsp.SkipPageException();				
			}		
		%>

		<center>
			<%-- response.setIntHeader("Refresh", 2); --%>
			<table>
				<tbody>
					<%	
					//Get current job PID if running
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

					out.println("<table>");
                	out.println("<tr>");
                	out.println("<td> Consolidator Job Process ID </td>");
                	out.println("<td>"+ currentConsolidatorJobPID + "</td>");
                	out.println("</tr>");
                	out.println("<tr>");
                	out.println("<td> Validator Job Process ID </td>");
                	out.println("<td>"+ currentValidatorJobPID + "</td>");
                	out.println("</tr>");
                	out.println("<tr>");
                	out.println("</tr>");
                	out.println("</table>");
					
                	                	
					Class.forName("org.sqlite.JDBC");
                
                	try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + validatorDBPath)) {
	                	try (Statement stat = conn.createStatement()) {		 
		                	ResultSet rs = stat.executeQuery("SELECT test_name, start_time, end_time, execution_time, status FROM test_results");		
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
			                	out.println("<td>" + rs.getString(1) + "</td>");
			                	out.println("<td>" + rs.getString(2) + "</td>");
			                	out.println("<td>" + rs.getString(3) + "</td>");
			                	out.println("<td>" + rs.getString(4) + "</td>");
			                	out.println("<td>" + rs.getString(5) + "</td>");
			                	out.println("</tr>");
			                }
		               	}
                	}
            	%>
				</tbody>
			</table>
		</center>
	</div>
</body>
</html>