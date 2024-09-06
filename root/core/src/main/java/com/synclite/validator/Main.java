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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {

	private static Path testRoot;
	private static Path loggerConfig;
	private static Path consolidatorConfig;
	private static Path corePath;
	private static Integer numThreads;
	
	public static void main(String[] args) {
		try {
			if (args.length != 11) {
				usage();
			} else {
				String cmd = args[0].trim().toLowerCase();
				if (! cmd.equals("test")) {
					usage();
				}				

				if (!args[1].trim().equals("--test-root")) {
					usage();
				} else {
					testRoot = Path.of(args[2]);
					if (!Files.exists(testRoot)) {
						error("Invalid test-root directory specified");
					}
				}

				if (!args[3].trim().equals("--logger-config")) {
					usage();
				} else {
					loggerConfig = Path.of(args[4]);
					if (!Files.exists(loggerConfig)) {
						error("Invalid logger-config file specified");
					}
				}				


				if (!args[5].trim().equals("--consolidator-config")) {
					usage();
				} else {
					consolidatorConfig  = Path.of(args[6]);
					if (!Files.exists(consolidatorConfig)) {
						error("Invalid consolidator-config file specified");
					}
				}

				if (!args[7].trim().equals("--core-path")) {
					usage();
				} else {
					corePath = Path.of(args[8]);
					if (!Files.exists(corePath)) {
						error("Invalid core-path specified");
					}
				}

				if (!args[9].trim().equals("--num-threads")) {
					usage();
				} else {
					try {
						numThreads = Integer.valueOf(args[10]);
					} catch (NumberFormatException e) {
						error("Invalid num-threads specified : " + e.getMessage());
					}
				}

			}

			//Start test execution			
			TestDriver testDriver = new TestDriver(testRoot, loggerConfig, consolidatorConfig, corePath, numThreads);
			testDriver.run();

		} catch (Exception e) {		
			try {				 
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				String stackTrace = sw.toString();
				Path exceptionFilePath; 
				Path workDir = testRoot.resolve("workDir");
				if (workDir == null) { 
					exceptionFilePath = Path.of("synclite_validator_exception.trace");
				} else {
					exceptionFilePath = workDir.resolve("synclite_validator_exception.trace");
				}

				String finalStr = e.getMessage() + "\n" + stackTrace;
				Files.writeString(exceptionFilePath, finalStr);
				System.out.println("ERROR : " + finalStr);
				System.err.println("ERROR : " + finalStr);	
			} catch (Exception ex) {
				System.out.println("ERROR : " + ex);
				System.err.println("ERROR : " + ex);	
			}
			System.exit(1);
		}

	}

	private static final void error(String message) throws Exception {
		System.out.println("ERROR : " + message);
		throw new Exception("ERROR : " + message);
	}

	private static final void usage() {
		System.out.println("ERROR : Usage: SyncLiteValidator test --test-root <path/to/test-root> --logger-config <path/to/logger-config> --consolidator-config <path/to/consolidator-config> --core-path <path/to/consolidator-clipath> --num-threads <number_of_threads>");
		System.exit(1);
	}
}
