package org.elasql.bench.workloads;

import org.elasql.storage.metadata.PartitionMetaMgr;

public class MultiTrendHotPartitionWorkload implements Workload {
	
	private static final int LONG_TERM_WINDOW_SIZE = 1000; // in milliseconds
	private static final int SHORT_TERM_WINDOW_SIZE = 20; // in milliseconds
	
	private static final int HOT_PARTITION_CHANGE_PERIOD = 150; // in window counts
	private static final int WORKLOAD_LENGTH = 1200;
	private static final double HOTNESS = 0.85;
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	public static final double[][] generateHotPartitionWorkload() {
		double[][] workload = new double[WORKLOAD_LENGTH][];
		
		for (int timeIdx = 0; timeIdx < WORKLOAD_LENGTH; timeIdx++) {
			int hotPart = timeIdx / HOT_PARTITION_CHANGE_PERIOD % NUM_PARTITIONS;
			double[] distribution = new double[NUM_PARTITIONS];
			double nonHotLoad = (1.0 - HOTNESS) / (NUM_PARTITIONS - 1);
			for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
				if (partId == hotPart) {
					distribution[partId] = HOTNESS;
				} else {
					distribution[partId] = nonHotLoad;
				}
			}
			workload[timeIdx] = distribution;
		}
		return workload;
	}
	
	private MultiTrendWorkload multiTrendWorkload;
	
	public MultiTrendHotPartitionWorkload() {
		double[][] workload = generateHotPartitionWorkload();
		multiTrendWorkload = new MultiTrendWorkload(workload, LONG_TERM_WINDOW_SIZE, SHORT_TERM_WINDOW_SIZE);
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
		return WORKLOAD_LENGTH * LONG_TERM_WINDOW_SIZE;
	}
}
