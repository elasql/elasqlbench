package org.elasql.bench.benchmarks.tpcc.rte;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.util.RandomValueGenerator;

public class ParamGenHelper {
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	private static final int LONG_TERM_WINDOW_SIZE = 1000; // in milliseconds
	private static final int SHORT_TERM_WINDOW_SIZE = 20; // in milliseconds
	private static final int SHORT_TERM_WINDOW_COUNT = LONG_TERM_WINDOW_SIZE / SHORT_TERM_WINDOW_SIZE;
	
	private static final double[][] WORKLOAD;
	private static final int[][] SHORT_TERM_SKEWNESS;
	
	private static final AtomicLong GLOBAL_START_TIME = new AtomicLong(0);
	// To delay replaying the workload (in milliseconds)
	private static final long DELAY_START_TIME = 00_000;
	
	static {
//		WORKLOAD = ElasqlYcsbConstants.loadGoogleWorkloadTrace(PartitionMetaMgr.NUM_PARTITIONS);
		
		double[][] dummyWorkload = new double[360][];
		for (int time = 0; time < 120; time++) {
			double[] distribution = new double[4];
			distribution[0] = 0.85;
			distribution[1] = 0.05;
			distribution[2] = 0.05;
			distribution[3] = 0.05;
			dummyWorkload[time] = distribution;
		}
		for (int time = 120; time < 240; time++) {
			double[] distribution = new double[4];
			distribution[0] = 0.05;
			distribution[1] = 0.85;
			distribution[2] = 0.05;
			distribution[3] = 0.05;
			dummyWorkload[time] = distribution;
		}
		for (int time = 240; time < 360; time++) {
			double[] distribution = new double[4];
			distribution[0] = 0.05;
			distribution[1] = 0.05;
			distribution[2] = 0.85;
			distribution[3] = 0.05;
			dummyWorkload[time] = distribution;
		}
		WORKLOAD = dummyWorkload;
		
		// Generate short-term skewness from the Google workload
		SHORT_TERM_SKEWNESS = new int[WORKLOAD.length][SHORT_TERM_WINDOW_COUNT];
		for (int timeIdx = 0; timeIdx < WORKLOAD.length; timeIdx++) {
			SHORT_TERM_SKEWNESS[timeIdx] = generateShortTermSequence(WORKLOAD[timeIdx], SHORT_TERM_WINDOW_COUNT);
		}
		
		// XXX: Double the skewness
//		for (int i = 0; i < WORKLOAD.length; i++) {
//			for (int j = 0; j < WORKLOAD[i].length; j++) {
//				WORKLOAD[i][j] = Math.pow(WORKLOAD[i][j], 2);
//			}
//		}
	}
	
	private static int[] generateShortTermSequence(double[] longTermDistribution, int seqLength) {
		// Calculate the sum
		double sum = 0.0;
		for (int i = 0; i < longTermDistribution.length; i++) {
			sum += longTermDistribution[i];
		}

		// Normalize the distribution to counts
		int[] counts = new int[longTermDistribution.length];
		for (int i = 0; i < counts.length; i++) {
			counts[i] = (int) (longTermDistribution[i] / sum * seqLength);
		}
		
		// Generate the sequence
		List<Integer> seqeuence = new ArrayList<Integer>(seqLength);
		for (int partId = 0; partId < counts.length; partId++) {
			Integer boxedPartId = partId;
			for (int i = 0; i < counts[partId]; i++)
				seqeuence.add(boxedPartId);
		}
		
		// Fill up the rest space (if there is any) with the last partition id
		while (seqeuence.size() < seqLength)
			seqeuence.add(counts.length - 1);
		
		// Shuffle to create randomness (deterministic)
		Collections.shuffle(seqeuence, new Random(12345678));
		
		// Save as an integer array
		int[] intArray = seqeuence.stream().mapToInt(Integer::intValue).toArray();
		
		return intArray;
	}
	
	private static long getElapsedTimeMs() {
		long startTime = GLOBAL_START_TIME.get();
		if (startTime == 0) {
			// Update by compare-and-set
			startTime = System.nanoTime();
			if (!GLOBAL_START_TIME.compareAndSet(0, startTime)) {
				startTime = GLOBAL_START_TIME.get();
			}
		}
		return (System.nanoTime() - startTime) / 1_000_000; // ns -> ms
	}
	
	private static int getCurrentReplayPoint() {
		long replayTime = getElapsedTimeMs() - DELAY_START_TIME;
		if (replayTime < 0) {
			return (int) (replayTime / LONG_TERM_WINDOW_SIZE - 1);
		} else {
			return (int) (replayTime / LONG_TERM_WINDOW_SIZE);
		}
	}
	
	private static int getCurrentFocusPartId() {
		long elapsedTime = getElapsedTimeMs() - DELAY_START_TIME;
		int longTermIdx = (int) (elapsedTime / LONG_TERM_WINDOW_SIZE);
		int timeSlotId = (int) (elapsedTime % LONG_TERM_WINDOW_SIZE / SHORT_TERM_WINDOW_SIZE);
		
		return SHORT_TERM_SKEWNESS[longTermIdx][timeSlotId];
	}
	
	private static boolean isReplaying() {
		int replayPoint = getCurrentReplayPoint();
		if (replayPoint >= 0 && replayPoint < WORKLOAD.length) {
			return true;
		}
		return false;
	}
	
	public static int getMainPartId() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		
		if (isReplaying()) { // Replay time
			return getCurrentFocusPartId();
		} else { // Non-replay time
			return rvg.number(0, NUM_PARTITIONS - 1);
		}
	}
	
	public static int getRemotePartId() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		
		if (isReplaying()) { // Replay time
			int replayPoint = getCurrentReplayPoint();
			return rvg.randomChooseFromDistribution(WORKLOAD[replayPoint]);
		} else { // Non-replay time
			return rvg.number(0, NUM_PARTITIONS - 1);
		}
	}
}
