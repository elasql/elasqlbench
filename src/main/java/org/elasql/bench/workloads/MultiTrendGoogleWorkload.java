package org.elasql.bench.workloads;

public class MultiTrendGoogleWorkload implements Workload {
	
	private MultiTrendWorkload multiTrendWorkload;
	
	public MultiTrendGoogleWorkload(int longTermWindowSize, int shortTermWindowSize) {
		double[][] googleWorkload = GoogleWorkload.loadGoogleWorkload();
		multiTrendWorkload = new MultiTrendWorkload(googleWorkload, longTermWindowSize, shortTermWindowSize);
	}

	@Override
	public int selectMainPartition(long currentTimeMs) {
		return multiTrendWorkload.selectMainPartition(currentTimeMs);
	}

	@Override
	public int selectRemotePartition(long currentTimeMs) {
		return multiTrendWorkload.selectRemotePartition(currentTimeMs);
	}

	@Override
	public int getWorkloadLengthMs() {
		return multiTrendWorkload.getWorkloadLengthMs();
	}
}
