/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.bench;

import org.elasql.bench.benchmarks.micro.ElasqlMicroBenchmarker;
import org.elasql.bench.benchmarks.tpcc.ElasqlTpccBenchmarker;
import org.elasql.bench.benchmarks.tpce.ElasqlTpceBenchmarker;
import org.elasql.bench.remote.sp.ElasqlSpDriver;
import org.vanilladb.bench.Benchmarker;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.remote.SutDriver;

public class App {

	private static int nodeId;
	private static int action;
	
	public static void main(String[] args) {
		Benchmarker benchmarker = null;
		
		try {
			parseArguments(args);
		} catch (IllegalArgumentException e) {
			System.out.println("Error: " + e.getMessage());
			System.out.println("Usage: ./app [Node Id] [Action]");
		}
		
		// Create a driver for connection
		SutDriver driver = null;
		switch (BenchmarkerParameters.CONNECTION_MODE) {
		case JDBC:
			throw new UnsupportedOperationException("ElaSQL does not support JDBC");
		case SP:
			driver = new ElasqlSpDriver(nodeId);
			break;
		}
		
		// Create a benchmarker
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			benchmarker = new ElasqlMicroBenchmarker(driver, nodeId);
			break;
		case TPCC:
			benchmarker = new ElasqlTpccBenchmarker(driver, nodeId);
			break;
		case TPCE:
			benchmarker = new ElasqlTpceBenchmarker(driver, nodeId);
			break;
		}
		
		switch (action) {
		case 1: // Load testbed
			benchmarker.loadTestbed();
			break;
		case 2: // Benchmarking
			benchmarker.benchmark();
			break;
		}
	}
	
	private static void parseArguments(String[] args) throws IllegalArgumentException {
		if (args.length < 2) {
			throw new IllegalArgumentException("The number of arguments is less than 2");
		}
		
		try {
			nodeId = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(String.format("'%s' is not a number", args[0]));
		}
		
		try {
			action = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(String.format("'%s' is not a number", args[1]));
		}
	}
}
