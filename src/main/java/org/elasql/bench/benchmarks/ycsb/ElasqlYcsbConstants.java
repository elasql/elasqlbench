package org.elasql.bench.benchmarks.ycsb;

import org.elasql.bench.util.ElasqlBenchProperties;

public class ElasqlYcsbConstants {
	
	// Database Mode
	public static enum DatabaseMode {
		SINGLE_TABLE, MULTI_TENANTS
	}
	public static final DatabaseMode DATABASE_MODE;
	
	public static final int INIT_RECORD_PER_PART;
	
	// Only works when using a multi-tenants db
	public static final int TENANTS_PER_PART;
	
	// 0: Normal, 100 is enough for underloaded
	public static final long SENDING_DELAY;
	
	// Workloads
	public static enum WorkloadType {
		NORMAL, SKEWED, GOOGLE
	}
	public static final WorkloadType WORKLOAD_TYPE;
	
	// Transaction characteristics
	public static final double RW_TX_RATE;
	public static final double DIST_TX_RATE;
	public static final int TX_RECORD_COUNT;
	public static final int REMOTE_RECORD_COUNT;
	public static final int ADD_INSERT_IN_WRITE_TX;
	
	// Zipfian
	public static final double ZIPFIAN_PARAMETER;
	
	static {
		// Database Mode
		int databaseMode = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlYcsbConstants.class.getName() + ".DATABASE_MODE", 1);
		switch (databaseMode) {
		case 1:
			DATABASE_MODE = DatabaseMode.SINGLE_TABLE;
			break;
		case 2:
			DATABASE_MODE = DatabaseMode.MULTI_TENANTS;
			break;
		default:
			throw new IllegalArgumentException("No database mode in YCSB for " + databaseMode);	
		}
		
		// Workloads
		int workloadType = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlYcsbConstants.class.getName() + ".WORKLOAD_TYPE", 1);
		switch (workloadType) {
		case 1:
			WORKLOAD_TYPE = WorkloadType.NORMAL;
			break;
		case 2:
			WORKLOAD_TYPE = WorkloadType.SKEWED;
			break;
		case 3:
			WORKLOAD_TYPE = WorkloadType.GOOGLE;
			break;
		default:
			throw new IllegalArgumentException("No YCSB workload for " + workloadType);	
		}
		
		SENDING_DELAY = ElasqlBenchProperties.getLoader()
				.getPropertyAsLong(ElasqlYcsbConstants.class.getName() + ".SENDING_DELAY", 0);
		INIT_RECORD_PER_PART = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".INIT_RECORD_PER_PART", 1_000_000);
		TENANTS_PER_PART = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".TENANTS_PER_PART", 1);
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".RW_TX_RATE", 0.2);
		DIST_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".DIST_TX_RATE", 0.0);
		TX_RECORD_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".TX_RECORD_COUNT", 2);
		REMOTE_RECORD_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".REMOTE_RECORD_COUNT", 1);
		ADD_INSERT_IN_WRITE_TX = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".ADD_INSERT_IN_WRITE_TX", 0);
		ZIPFIAN_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".ZIPFIAN_PARAMETER", 0.99);
	}
}
