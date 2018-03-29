package org.elasql.bench.rte.ycsb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.Benchmarker;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.tpcc.TpccValueGenerator;
import org.vanilladb.bench.util.YcsbLatestGenerator;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.bench.ycsb.YcsbTransactionType;

public class ElasqlYcsbRealisticOverallParamGen implements TxParamGenerator {
	private static final double RW_TX_RATE;
	private static final double DIST_TX_RATE;
	private static final double SKEW_PARAMETER;
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;

	private static final AtomicInteger[] GLOBAL_COUNTERS;

	// Real parameter
	private static final int DATA_LEN = 51;
	private static double DATA[][] = new double[NUM_PARTITIONS][DATA_LEN]; // [Partition][Time]

	private static AtomicLong globalStartTime = new AtomicLong(-1);
	private static final long REPLAY_PREIOD;
	private static final long WARMUP_TIME;
//	private static final double SKEW_WEIGHT;

	private static int nodeId;

	private static final AtomicReference<YcsbLatestGenerator> GLOBAL_GEN;

	static {
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.0);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.0);

		DIST_TX_RATE = 0.1;

		WARMUP_TIME = 120 * 1000;
		REPLAY_PREIOD = 204 * 1000;
//		SKEW_WEIGHT = 6.5;

		// Get data from Google Cluster
		// Directly choose 1 ~ NUM_PARTITIONS workloads
		// int target[] = new int[NUM_PARTITIONS];
		// for (int i = 0; i < NUM_PARTITIONS; i++)
		// target[i] = i+1;
		// Assign the chosen workloads
		int target[] = {
				1348, 11384, 11956, 9768, 8515, 8962, 4900, // Former Skews
				12122, 11112, 5038, 8292, 3165, 9572, 316, // Later Skews
				8622, 9441, 4967, 5235, 1670, 2748 // Stables
		};

		// Read data
		try (BufferedReader reader = new BufferedReader(new FileReader("/opt/shared/Google_Cluster_Data.csv"))) {
			// Data Format: Each row is a workload of a node, each value is the
			// load at a time point
			String line = reader.readLine();
			String loads[];
			int partId = 0;
			int row = 0;
			boolean hit = false;
			int hitCount = 0;

			while (hitCount < NUM_PARTITIONS && line != null) {
				// Find the target row
				hit = false;
				for (partId = 0; partId < NUM_PARTITIONS; partId++) {
					if (row == target[partId]) {
						hit = true;
						hitCount++;
						break;
					}
				}

				// Record the loads
				if (hit) {
//					System.out.println(line);
					loads = line.split(",");
					for (int j = 0; j < loads.length; j++) {
						DATA[partId][j] = Double.parseDouble(loads[j]);
					}
				}

				// Read next line
				line = reader.readLine();
				hit = false;
				row++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		GLOBAL_GEN = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.RECORD_PER_PART, SKEW_PARAMETER));
	}

	static {
		if (NUM_PARTITIONS == -1)
			throw new RuntimeException("it's -1 !!!!");

		GLOBAL_COUNTERS = new AtomicInteger[NUM_PARTITIONS];
		for (int i = 0; i < NUM_PARTITIONS; i++)
			GLOBAL_COUNTERS[i] = new AtomicInteger(0);
	}

	private static long getGlobalStartTime() {
		long time = globalStartTime.get();
		if (time == -1) {
			globalStartTime.compareAndSet(-1, System.currentTimeMillis());
			time = globalStartTime.get();
		}
		return time;
	}

	private static int getNextInsertId(int partitionId) {
		int id = GLOBAL_COUNTERS[partitionId].getAndIncrement();
		int CLIENT_COUNT = NUM_PARTITIONS;

		return id * CLIENT_COUNT + nodeId + getStartId(partitionId) + ElasqlYcsbConstants.RECORD_PER_PART;
	}

	private static int getStartId(int partitionId) {
		return partitionId * ElasqlYcsbConstants.MAX_RECORD_PER_PART + 1;
	}

	private YcsbLatestGenerator[] latestRandoms = new YcsbLatestGenerator[NUM_PARTITIONS];
	private long startTime = -1;
	private boolean notifyReplayStart, notifyReplayEnd;

	public ElasqlYcsbRealisticOverallParamGen(int nodeId) {
		ElasqlYcsbRealisticOverallParamGen.nodeId = nodeId;
		for (int i = 0; i < NUM_PARTITIONS; i++) {
			latestRandoms[i] = new YcsbLatestGenerator(GLOBAL_GEN.get());
		}
	}

	@Override
	public TransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}

	@Override
	public Object[] generateParameter() {
		TpccValueGenerator rvg = new TpccValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();

		if (startTime == -1) {
			// NOTE: getGlobalStartTime() gives the real start time of
			// benchmarking
			startTime = getGlobalStartTime();

			// NOTE: BENCH_START_TIME indicates the launch time of entire
			// program
			long currentTime = (System.nanoTime() - Benchmarker.BENCH_START_TIME) / 1_000_000_000;
			System.out.println("Benchmark starts at " + currentTime);
		}

		// ================================
		// Decide the types of transactions
		// ================================

		boolean isDistributedTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0) ? true
				: false;
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;

		if (NUM_PARTITIONS < 2)
			isDistributedTx = false;

		/////////////////////////////

		// =========================================
		// Decide the counts and the main partitions
		// =========================================

		// Choose the main partition
		int mainPartition = 0;

		long pt = (System.currentTimeMillis() - startTime) - WARMUP_TIME;
		int timePoint = (int) (pt / (REPLAY_PREIOD / DATA_LEN));

		// Replay time
		if (pt > 0 && timePoint >= 0 && timePoint < DATA_LEN) {
			mainPartition = genDistributionOfPart(timePoint, rvg);
			// System.out.println("pt " + timePoint);

			if (!notifyReplayStart) {
				long currentTime = (System.nanoTime() - Benchmarker.BENCH_START_TIME) / 1_000_000_000;
				System.out.println("Replay starts at " + currentTime);
				System.out.println("Estimated time point: " + timePoint);
				notifyReplayStart = true;
			}
		} else { // Non-replay time
			mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
			// System.out.println("Choose " + mainPartition);

			if (notifyReplayStart && !notifyReplayEnd) {
				long currentTime = (System.nanoTime() - Benchmarker.BENCH_START_TIME) / 1_000_000_000;
				System.out.println("Replay ends at " + currentTime);
				notifyReplayEnd = true;
			}
		}

		// Decide counts
		int readCount;
		int localReadCount = 2;
		int remoteReadCount = 2;

		if (isDistributedTx)
			readCount = localReadCount + remoteReadCount;
		else
			readCount = localReadCount;

		// =====================
		// Generating Parameters
		// =====================
		int[] readRemoteId = new int[remoteReadCount];

		if (isDistributedTx) {
			for (int i = 0; i < remoteReadCount; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition, rvg);
				readRemoteId[i] = chooseARecordInMainPartition(remotePartition);
			}
		}

		if (isReadWriteTx) {
			int readWriteId = chooseARecordInMainPartition(mainPartition);
			int insertId = getNextInsertId(mainPartition);

			// Read count
			paramList.add(readCount);

			// Read ids (in integer)
			paramList.add(readWriteId);
			for (int i = 1; i < localReadCount; i++) {
				paramList.add(chooseARecordInMainPartition(mainPartition));
			}

			if (isDistributedTx) {
				for (int i = 0; i < remoteReadCount; i++) {
					paramList.add(readRemoteId[i]);
				}
			}

			// Write count
			paramList.add(1);

			// Write ids (in integer)
			paramList.add(readWriteId);

			// Write values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));

			// Insert count
			paramList.add(0);

			// Insert ids (in integer)
			paramList.add(insertId);

			// Insert values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));

		} else {
			// Read count
			paramList.add(readCount);

			for (int i = 0; i < localReadCount; i++) {
				paramList.add(chooseARecordInMainPartition(mainPartition));
			}

			if (isDistributedTx) {
				for (int i = 0; i < remoteReadCount; i++) {
					paramList.add(readRemoteId[i]);
				}
			}
			// Write count
			paramList.add(0);

			// Insert count
			paramList.add(0);
		}

		return paramList.toArray(new Object[0]);
	}

	private int randomChooseOtherPartition(int mainPartition, TpccValueGenerator rvg) {
		return ((mainPartition + rvg.number(1, NUM_PARTITIONS - 1)) % NUM_PARTITIONS);
	}

	private int chooseARecordInMainPartition(int mainPartition) {
		int partitionStartId = getStartId(mainPartition);

		return (int) latestRandoms[mainPartition].nextValue() + partitionStartId - 1;
	}

	private int genDistributionOfPart(int time, TpccValueGenerator rvg) {
		LinkedList<Integer> permutations = new LinkedList<Integer>();
		int amplifyScale = 100;
		double total = 0;

		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
			total += DATA[partId][time];
		}

		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
			for (int j = 0; j < amplifyScale * DATA[partId][time] / total; j++)
				permutations.add(partId);
		}

		return permutations.get(rvg.number(0, permutations.size() - 1));
	}
}
