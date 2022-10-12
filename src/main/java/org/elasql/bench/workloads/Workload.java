package org.elasql.bench.workloads;

public interface Workload {
	
	int selectMainPartition(long currentTimeMs);
	
	int selectRemotePartition(long currentTimeMs);
	
	int getWorkloadLengthMs();
}
