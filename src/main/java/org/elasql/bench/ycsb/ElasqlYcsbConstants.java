package org.elasql.bench.ycsb;

import org.elasql.bench.util.ElasqlBenchProperties;

public class ElasqlYcsbConstants {
	
	public static final int RECORD_PER_PART = 10_000_000;
	public static final int MAX_RECORD_PER_PART = 100_000_000; // for insertion
	
	public static enum WorkloadType {
		GOOGLE, MULTI_TENANTS
	}
	public static final WorkloadType WORKLOAD_TYPE;
	
	static {
		int workloadType = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlYcsbConstants.class.getName() + ".WORKLOAD_TYPE", 1);
		switch (workloadType) {
		case 1:
			WORKLOAD_TYPE = WorkloadType.GOOGLE;
			break;
		case 2:
			WORKLOAD_TYPE = WorkloadType.MULTI_TENANTS;
			break;
		default:
			throw new IllegalArgumentException("No YCSB workload for " + workloadType);	
		}
	}

}
