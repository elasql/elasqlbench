package org.elasql.bench.benchmarks.tpcc.rte;

import java.util.concurrent.atomic.AtomicLong;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.util.RandomValueGenerator;

public class ParamGenHelper {
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	private static final double[][] WORKLOAD;
	private static final AtomicLong GLOBAL_START_TIME = new AtomicLong(0);
	// To delay replaying the workload (in milliseconds)
	private static final long DELAY_START_TIME = 90_000;
	
	static {
		WORKLOAD = ElasqlYcsbConstants.loadGoogleWorkloadTrace(PartitionMetaMgr.NUM_PARTITIONS);
	}
	
	public static int getCurrentReplayPoint() {
		long startTime = GLOBAL_START_TIME.get();
		if (startTime == 0) {
			// Update by compare-and-set
			startTime = System.nanoTime();
			if (!GLOBAL_START_TIME.compareAndSet(0, startTime)) {
				startTime = GLOBAL_START_TIME.get();
			}
		}
		long elapsedTime = (System.nanoTime() - startTime) / 1_000_000; // ns -> ms
		return (int) ((elapsedTime - DELAY_START_TIME) / 1000);
	}
	
	public static int getPartId() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		
		// Check the current time point
		int replayPoint = getCurrentReplayPoint();
		boolean isReplaying = false;
		if (replayPoint >= 0 && replayPoint < WORKLOAD.length) {
			isReplaying = true;
		}
		
		// Select a partition based on the distribution of the workload at the given time
		int mainPartId;
		if (isReplaying) { // Replay time
			mainPartId = rvg.randomChooseFromDistribution(WORKLOAD[replayPoint]);
		} else { // Non-replay time
			mainPartId = rvg.number(0, NUM_PARTITIONS - 1);
		}
		
		return mainPartId;
	}
}
