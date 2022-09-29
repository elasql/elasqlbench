package org.elasql.bench.benchmarks.ycsb;

import org.elasql.bench.util.ElasqlBenchProperties;

public class ElasqlYcsbConstants {
	
	// Database Mode
	public static enum DatabaseMode {
		SINGLE_TABLE, MULTI_TABLE
	}
	public static final DatabaseMode DATABASE_MODE;
	
	public static final int INIT_RECORD_PER_PART;
	
	// Only works when using a multi-tenants db
	public static final int TENANTS_PER_PART;
	
	// 0: Normal, 100 is enough for underloaded
	public static final long SENDING_DELAY;
	
	// Workloads
	public static enum WorkloadType {
		NORMAL, GOOGLE, MULTI_TENANT, HOT_COUNTER
	}
	public static final WorkloadType WORKLOAD_TYPE;
	
	// Transaction characteristics
	public static final double RW_TX_RATE;
	public static final double DIST_TX_RATE;
	public static final double REMOTE_RECORD_RATIO;
	public static final int ADD_INSERT_IN_WRITE_TX;
	public static final boolean USE_DYNAMIC_RECORD_COUNT;
	
	// Hotspot
	public static final boolean ENABLE_HOTSPOT;
	public static final double HOTSPOT_HOTNESS;
	public static final int HOTSPOT_CHANGE_PERIOD; // in seconds
	
	// Fixed record count
	public static final int TX_RECORD_COUNT;
	
	// Dynamic record count
	public static final int RECORD_COUNT_MEAN;
	public static final int RECORD_COUNT_STD;
	public static final int DYNAMIC_RECORD_COUNT_RANGE;
	
	// Zipfian
	public static final double ZIPFIAN_PARAMETER;
	
	// Hot Counter
	public static final int HOT_COUNT_PER_PART;
	public static final double HOT_UPDATE_RATE_IN_RW_TX;
	
	static {
		// Database Mode
		int databaseMode = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlYcsbConstants.class.getName() + ".DATABASE_MODE", 1);
		switch (databaseMode) {
		case 1:
			DATABASE_MODE = DatabaseMode.SINGLE_TABLE;
			break;
		case 2:
			DATABASE_MODE = DatabaseMode.MULTI_TABLE;
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
			WORKLOAD_TYPE = WorkloadType.GOOGLE;
			break;
		case 3:
			WORKLOAD_TYPE = WorkloadType.MULTI_TENANT;
			break;
		case 4:
			WORKLOAD_TYPE = WorkloadType.HOT_COUNTER;
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
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".RW_TX_RATE", 0.5);
		DIST_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".DIST_TX_RATE", 0.5);
		USE_DYNAMIC_RECORD_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsBoolean(ElasqlYcsbConstants.class.getName() + ".USE_DYNAMIC_RECORD_COUNT", false);
		ENABLE_HOTSPOT = ElasqlBenchProperties.getLoader()
				.getPropertyAsBoolean(ElasqlYcsbConstants.class.getName() + ".ENABLE_HOTSPOT", false);
		HOTSPOT_HOTNESS = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".HOTSPOT_HOTNESS", 0.9);
		HOTSPOT_CHANGE_PERIOD = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".HOTSPOT_CHANGE_PERIOD", 90);
		TX_RECORD_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".TX_RECORD_COUNT", 2);
		REMOTE_RECORD_RATIO = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".REMOTE_RECORD_RATIO", 0.5);
		ADD_INSERT_IN_WRITE_TX = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".ADD_INSERT_IN_WRITE_TX", 0);
		ZIPFIAN_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".ZIPFIAN_PARAMETER", 0.99);
		RECORD_COUNT_MEAN = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".RECORD_COUNT_MEAN", 20);
		RECORD_COUNT_STD = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".RECORD_COUNT_STD", 10);
		HOT_COUNT_PER_PART = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".HOT_COUNT_PER_PART", 1);
		HOT_UPDATE_RATE_IN_RW_TX = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbConstants.class.getName() + ".HOT_UPDATE_RATE_IN_RW_TX", 0.1);
		DYNAMIC_RECORD_COUNT_RANGE = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".DYNAMIC_RECORD_COUNT_RANGE", 5);
	}
}
