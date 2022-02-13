package org.elasql.bench;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.util.ElasqlBenchProperties;

public class ElasqlBenchParameters {
	private static Logger logger = Logger.getLogger(ElasqlBenchParameters.class
			.getName());
	
	public static final long WARM_UP_INTERVAL;
	public static final long BENCHMARK_INTERVAL;
	public static final int NUM_RTES;
	public static final long RTE_SLEEP_TIME;
	
	// Micro = 1, TPC-C = 2, TPC-E = 3, YCSB = 4, RECON = 5
	public static enum BenchType { MICRO, TPCC, TPCE, YCSB, RECON };
	public static final BenchType BENCH_TYPE;
	
	public static final boolean PROFILING_ON_SERVER;
	
	public static final File REPORT_OUTPUT_DIRECTORY;
	public static final int REPORT_TIMELINE_GRANULARITY;
	
	public static final boolean SHOW_TXN_RESPONSE_ON_CONSOLE;

	static {
		WARM_UP_INTERVAL = ElasqlBenchProperties.getLoader().getPropertyAsLong(
				ElasqlBenchParameters.class.getName() + ".WARM_UP_INTERVAL", 60000);

		BENCHMARK_INTERVAL = ElasqlBenchProperties.getLoader().getPropertyAsLong(
				ElasqlBenchParameters.class.getName() + ".BENCHMARK_INTERVAL",
				60000);

		NUM_RTES = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlBenchParameters.class.getName() + ".NUM_RTES", 1);
		
		RTE_SLEEP_TIME = ElasqlBenchProperties.getLoader().getPropertyAsLong(
				ElasqlBenchParameters.class.getName() + ".RTE_SLEEP_TIME", 0);

		int benchType = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlBenchParameters.class.getName() + ".BENCH_TYPE", 1);
		switch (benchType) {
		case 1:
			BENCH_TYPE = BenchType.MICRO;
			break;
		case 2:
			BENCH_TYPE = BenchType.TPCC;
			break;
		case 3:
			BENCH_TYPE = BenchType.TPCE;
			break;
		case 4:
			BENCH_TYPE = BenchType.YCSB;
			break;
		case 5:
			BENCH_TYPE = BenchType.RECON;
			break;
		default:
			throw new IllegalArgumentException("The connection mode should be 1 (Micro), 2 (TPC-C), or 3 (TPC-E)");
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Using " + BENCH_TYPE + " benchmarks");
		
		PROFILING_ON_SERVER = ElasqlBenchProperties.getLoader().getPropertyAsBoolean(
				ElasqlBenchParameters.class.getName() + ".PROFILING_ON_SERVER", false);
		
		// Report Output Directory
		String outputDirPath = ElasqlBenchProperties.getLoader()
				.getPropertyAsString(ElasqlBenchParameters.class.getName() + ".REPORT_OUTPUT_DIRECTORY", null);

		if (outputDirPath == null) {
			REPORT_OUTPUT_DIRECTORY = new File(System.getProperty("user.home"), "benchmark_results");
		} else {
			REPORT_OUTPUT_DIRECTORY = new File(outputDirPath);
		}

		// Create the directory if that doesn't exist
		if (!REPORT_OUTPUT_DIRECTORY.exists())
			REPORT_OUTPUT_DIRECTORY.mkdir();

		REPORT_TIMELINE_GRANULARITY = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlBenchParameters.class.getName() + ".REPORT_TIMELINE_GRANULARITY", 3000);
		
		SHOW_TXN_RESPONSE_ON_CONSOLE = ElasqlBenchProperties.getLoader().getPropertyAsBoolean(
				ElasqlBenchParameters.class.getName() + ".SHOW_TXN_RESPONSE_ON_CONSOLE", false);
	}
}
