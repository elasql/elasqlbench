package org.elasql.bench.benchmarks.ycsb.rte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.PeriodicalJob;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
import org.vanilladb.bench.benchmarks.ycsb.rte.YcsbLatestGenerator;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;

/**
 * Parameter format: [1, read count, (read id array), write count,
 * (write id array), (write value array), insert count = 0]<br>
 * <br>
 * Single-table does not support insertions.
 * 
 * @author yslin
 */
public class SingleTableGoogleParamGen implements TxParamGenerator<YcsbTransactionType> {
	private static Logger logger = Logger.getLogger(SingleTableGoogleParamGen.class.getName());
	
	private static final double RW_TX_RATE = ElasqlYcsbConstants.RW_TX_RATE;
	private static final double DIST_TX_RATE = ElasqlYcsbConstants.DIST_TX_RATE;

	private static final boolean USE_DYNAMIC_RECORD_COUNT = ElasqlYcsbConstants.USE_DYNAMIC_RECORD_COUNT;
	private static final int TOTAL_RECORD_COUNT = ElasqlYcsbConstants.TX_RECORD_COUNT;
	private static final int REMOTE_RECORD_COUNT = (int) (ElasqlYcsbConstants.TX_RECORD_COUNT * 
			ElasqlYcsbConstants.REMOTE_RECORD_RATIO);
	private static final int RECORD_COUNT_MEAN = ElasqlYcsbConstants.RECORD_COUNT_MEAN;
	private static final int RECORD_COUNT_STD = ElasqlYcsbConstants.RECORD_COUNT_STD;
	
	// How many rounds that the global skew moves from the left to the right
	private static final int GLOBAL_SKEW_REPEAT = 3;
	private static final AtomicReference<TwoSidedSkewGenerator> TWO_SIDED_ZIP_TEMPLATE;
	private static final AtomicReference<YcsbLatestGenerator> ZIP_TEMPLATE;
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	private static final int DATABASE_SIZE = ElasqlYcsbConstants.INIT_RECORD_PER_PART * NUM_PARTITIONS;
	
	private static final double[][] WORKLOAD;
	private static final int LONG_TERM_WINDOW_SIZE = 1000; // in milliseconds
	private static final int SHORT_TERM_WINDOW_SIZE = 20; // in milliseconds
	private static final int SHORT_TERM_WINDOW_COUNT = LONG_TERM_WINDOW_SIZE / SHORT_TERM_WINDOW_SIZE;
	private static final int[][] SHORT_TERM_SKEWNESS;
	private static final AtomicLong GLOBAL_START_TIME = new AtomicLong(0);
	
	// To delay replaying the workload (in milliseconds)
	private static final long DELAY_START_TIME = 90_000;
	
	static {
		WORKLOAD = ElasqlYcsbConstants.loadGoogleWorkloadTrace(PartitionMetaMgr.NUM_PARTITIONS);
		TWO_SIDED_ZIP_TEMPLATE = new AtomicReference<TwoSidedSkewGenerator>(
				new TwoSidedSkewGenerator(DATABASE_SIZE, ElasqlYcsbConstants.ZIPFIAN_PARAMETER));
		ZIP_TEMPLATE = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.INIT_RECORD_PER_PART,
						ElasqlYcsbConstants.ZIPFIAN_PARAMETER));
		
		// Generate short-term skewness from the Google workload
		SHORT_TERM_SKEWNESS = new int[WORKLOAD.length][SHORT_TERM_WINDOW_COUNT];
		for (int timeIdx = 0; timeIdx < WORKLOAD.length; timeIdx++) {
			SHORT_TERM_SKEWNESS[timeIdx] = generateShortTermSequence(WORKLOAD[timeIdx], SHORT_TERM_WINDOW_COUNT);
		}
		
		if (logger.isLoggable(Level.INFO)) {
			String recordStr = "";
			if (USE_DYNAMIC_RECORD_COUNT) {
				recordStr = String.format("use dynamic record count with mean = %d and std = %d",
						RECORD_COUNT_MEAN, RECORD_COUNT_STD);
			} else {
				recordStr = String.format("%d records/tx, %d remote records/dist. tx",
						TOTAL_RECORD_COUNT, REMOTE_RECORD_COUNT);
			}
			logger.info(String.format("Use single-table Google YCSB generators "
					+ "(Read-write tx ratio: %f, distributed tx ratio: %f, "
					+ "%s, data size: %d, google trace file: %s, google trace length: %d)",
					RW_TX_RATE, DIST_TX_RATE, recordStr, DATABASE_SIZE,
					ElasqlYcsbConstants.GOOGLE_TRACE_FILE,
					ElasqlYcsbConstants.GOOGLE_TRACE_LENGTH));
		}
		
		// Debug: trace the current replay time
		new PeriodicalJob(5000, BenchmarkerParameters.BENCHMARK_INTERVAL, new Runnable() {
			@Override
			public void run() {
				// Wait for the start time set
				if (GLOBAL_START_TIME.get() == 0) {
					return;
				}
				
				int replayPoint = getCurrentReplayPoint();

				if (replayPoint >= 0 && replayPoint < WORKLOAD.length) {
					System.out.println(String.format("Replaying. Current replay point: %d", replayPoint));
				} else {
					System.out.println(String.format("Not replaying. Current replay point: %d", replayPoint));
				}
			}
		}).start();
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
	
	// XXX: Debug
//	private static AtomicInteger lastLtIdx = new AtomicInteger(-1);
//	private static AtomicInteger lastStIdx = new AtomicInteger(-1);
	
	private static int getCurrentFocusPartId() {
		long elapsedTime = getElapsedTimeMs() - DELAY_START_TIME;
		int longTermIdx = (int) (elapsedTime / LONG_TERM_WINDOW_SIZE);
		int timeSlotId = (int) (elapsedTime % LONG_TERM_WINDOW_SIZE / SHORT_TERM_WINDOW_SIZE);
		
		// XXX: Debug: show long term distribution
//		int lastLtId = lastLtIdx.get();
//		if (lastLtId != longTermIdx) {
//			if (lastLtIdx.compareAndSet(lastLtId, longTermIdx)) {
//				System.out.println(String.format("Time: %d ms, Long-Term %d, Dist: %s", elapsedTime, longTermIdx, Arrays.toString(WORKLOAD[longTermIdx])));
//			}
//		}
		
		// XXX: Debug: show current short-term focus
//		int lastStId = lastStIdx.get();
//		if (lastStId != timeSlotId) {
//			if (lastStIdx.compareAndSet(lastStId, timeSlotId)) {
//				System.out.println(String.format("Time: %d ms, Short-Term %d, Part Id: %d", elapsedTime, timeSlotId, SHORT_TERM_SKEWNESS[longTermIdx][timeSlotId]));
//			}
//		}
		
		return SHORT_TERM_SKEWNESS[longTermIdx][timeSlotId];
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
	
	private TwoSidedSkewGenerator twoSidedZipGenerator;
	private YcsbLatestGenerator zipfianGenerator;
	private int[] shortTermTimeSlotSequence;
	private int currentReplayPoint = -1;
	
	public SingleTableGoogleParamGen() {
		this.twoSidedZipGenerator = new TwoSidedSkewGenerator(TWO_SIDED_ZIP_TEMPLATE.get());
		this.zipfianGenerator = new YcsbLatestGenerator(ZIP_TEMPLATE.get());
	}

	@Override
	public YcsbTransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		
		// Check the current time point
		int replayPoint = getCurrentReplayPoint();
		boolean isReplaying = false;
		if (replayPoint >= 0 && replayPoint < WORKLOAD.length) {
			isReplaying = true;
		}

		// Decide the types of transactions
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0);
		boolean isDistTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0);
		if (NUM_PARTITIONS < 2 || !isReplaying)
			isDistTx = false;
		
		// Select a partition based on the distribution of the workload at the given time
		int mainPartId;
		if (isReplaying) { // Replay time
//			mainPartId = rvg.randomChooseFromDistribution(WORKLOAD[replayPoint]);
			mainPartId = getCurrentFocusPartId();
		} else { // Non-replay time
			mainPartId = rvg.number(0, NUM_PARTITIONS - 1);
		}

		// Generate parameters
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(1); // dbtype = 1 (single-table)
		
		// Decide the number of records
		int totalRecordCount = TOTAL_RECORD_COUNT;
		int remoteRecordCount = REMOTE_RECORD_COUNT;
		if (USE_DYNAMIC_RECORD_COUNT) {
			double zeroMeanRandom = rvg.rng().nextGaussian();
			int randomCount = (int) (RECORD_COUNT_MEAN + zeroMeanRandom * RECORD_COUNT_STD);
			if (randomCount <= 1)
				randomCount = 2;
			if (randomCount > 50)
				randomCount = 50;
			
			totalRecordCount = randomCount;
			remoteRecordCount = randomCount / 2;
		}
		int localRecordCount = totalRecordCount;
		if (isDistTx) {
			localRecordCount -= remoteRecordCount;
		}
		
		// Read count
		paramList.add(totalRecordCount);
		
		// Read ids
		ArrayList<Long> ids = new ArrayList<Long>();
		
		// Local reads
		chooseRecordsInPart(mainPartId, localRecordCount, ids);
		
		// Remote reads
		if (isDistTx && remoteRecordCount > 0)
			chooseGlobalRecords(replayPoint, remoteRecordCount, ids);
		
		// Add the ids to the param list
		for (Long id : ids)
			paramList.add(id);
		
		// For read-write transactions
		if (isReadWriteTx) {
			// Write count
			paramList.add(totalRecordCount);
			
			// Write ids
			for (Long id : ids)
				paramList.add(id);
			
			// Write values
			for (int i = 0; i < totalRecordCount; i++)
				paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
			// Insert count
			// Single-table does not support insertion
			paramList.add(0);
		} else {
			// Write count
			paramList.add(0);
			// Insert count
			paramList.add(0);
		}
		
		return paramList.toArray(new Object[paramList.size()]);
	}
	
	private void chooseRecordsInPart(int partId, int count, ArrayList<Long> ids) {
		for (int i = 0; i < count; i++) {
			long id = chooseKeyInPart(partId);
			while (!ids.add(id))
				id = chooseKeyInPart(partId);
		}
	}
	
	private void chooseGlobalRecords(int replayTime, int count, ArrayList<Long> ids) {
		// Choose the center
		int center = DATABASE_SIZE / 2;
		if (replayTime >= 0 && replayTime < WORKLOAD.length) {
			// Note that it might be overflowed here.
			// The center of the 2-sided distribution changes
			// as the time increases. It moves from 0 to DATA_SIZE
			// and bounces back when it hits the end of the range. 
			int windowSize = WORKLOAD.length / GLOBAL_SKEW_REPEAT;
			int timeOffset = replayTime % (2 * windowSize);
			if (timeOffset >= windowSize)
				timeOffset = 2 * windowSize - timeOffset;
			center = DATABASE_SIZE / windowSize;
			center *= ((timeOffset % windowSize) + 1); 
		}
		
		// Use a global Zipfian distribution to select records
		for (int i = 0; i < count; i++) {
			long id = twoSidedZipGenerator.nextValue(center);
			while (!ids.add(id))
				id = twoSidedZipGenerator.nextValue(center);
		}
	}
	
	private long chooseKeyInPart(int partId) {
		long partStartId = partId * ElasqlYcsbConstants.INIT_RECORD_PER_PART;
		long offset = zipfianGenerator.nextValue();
		return partStartId + offset;
	}
}
