package org.elasql.bench.rte.ycsb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.PeriodicalJob;
import org.vanilladb.bench.Benchmarker;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.tpcc.TpccValueGenerator;
import org.vanilladb.bench.util.YcsbLatestGenerator;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.bench.ycsb.YcsbTransactionType;

public class GoogleWorkloadsParamGen implements TxParamGenerator {

	private static final double RW_TX_RATE;
	private static final double DIST_TX_RATE;
	private static final double SKEW_PARAMETER;

	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	private static final int DATA_SIZE = ElasqlYcsbConstants.RECORD_PER_PART * NUM_PARTITIONS;

	private static final int TOTAL_READ_COUNT = 2;
	private static final int REMOTE_READ_COUNT = 1;

	private static final AtomicInteger[] GLOBAL_COUNTERS;

	// Real parameter
	private static final int DATA_LEN = 51;
	private static double DATA[][] = new double[DATA_LEN][NUM_PARTITIONS]; // [Time][Partition]

	private static AtomicLong globalStartTime = new AtomicLong(-1);
	private static final long REPLAY_PREIOD;
	public static final long WARMUP_TIME;
	// private static final double SKEW_WEIGHT;

	private static int nodeId;

	private static final AtomicReference<YcsbLatestGenerator> STATIC_GEN_FOR_PART;
	private static final AtomicReference<TwoSidedSkewGenerator> STATIC_GLOBAL_GEN;

	static {
		DIST_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".DIST_TX_RATE", 0.5);
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.5);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.0);

		WARMUP_TIME = 90 * 1000;
		REPLAY_PREIOD = 255 * 1000;
		// SKEW_WEIGHT = 6.5;

		// Get data from Google Cluster
		// Directly choose 1 ~ NUM_PARTITIONS workloads
//		int target[] = new int[NUM_PARTITIONS];
//		for (int i = 0; i < NUM_PARTITIONS; i++)
//			target[i] = i + 1;
		// Assign the chosen workloads (for 20 nodes)
		int target[] = {
			9768, 8962, 4179, 12070, 6737, 4509, 11475, 11898, 11384, 4900, // Former Skews
			3165, 7733, 1359, 9572, 1958, 5038, 12122, 10304, 316, 4019, // Later Skews
			// Stables
		};
		// Workloads 2
//		int target[] = { 4179, 4509, 4900, 6737, // Former Skews
//				400, 2202, 2356, 2384, // Middle Skews
//				316, 1359, 1958, 3165, // Later Skews
//				422, 558, 805, 1721, 3530, 4337, 5272, 6406, // Low loading
//		};
		// Workloads 3
		// int target[] = {
		// 4179, 4509, // Former Skews
		// 400, 3584, // Middle Skews
		// 1359, 3165, // Later Skews
		// 311, 422, 558, 619, 805, 1721, 3530, 4337, 5272, 6406, 6491, 8040,
		// 8202, 8630, // Low loading
		// };
		// Assign the chosen workloads (for 8 nodes)
		// int target[] = {
		// 11475, 11898, 11384, 4900, // Former Skews
		// 3165, 7733, 1359, 9572, // Later Skews
		// // Stables
		// };

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
					// System.out.println(line);
					loads = line.split(",");
					for (int j = 0; j < loads.length; j++) {
						DATA[j][partId] = Double.parseDouble(loads[j]);
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

		// Clear the loading of the last partition
		for (int time = 0; time < DATA_LEN; time++) {
			DATA[time][NUM_PARTITIONS - 1] = 0.0;
		}

		// Another alter distribution
//		int oneThird = DATA_LEN / 3;
//		int twoThird = 2 * oneThird;
//		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
//			for (int time = 0; time < DATA_LEN; time++) {
//				if (partId % 2 == 0) {
//					if (time < oneThird) {
//						DATA[time][partId] = 1.0;
//					} else if (time < twoThird) {
//						int diff = time - oneThird;
//						DATA[time][partId] = 1.0 - 0.9 * diff / oneThird;
//					} else {
//						DATA[time][partId] = 0.1;
//					}
//				} else {
//					if (time < oneThird) {
//						DATA[time][partId] = 0.1;
//					} else if (time < twoThird) {
//						int diff = time - oneThird;
//						DATA[time][partId] = 0.1 + 0.9 * diff / oneThird;
//					} else {
//						DATA[time][partId] = 1.0;
//					}
//				}
//			}
//		}

		// Alter the data distribution for testing
//		int oneThird = DATA_LEN / 3;
//		int twoThird = 2 * oneThird;
//		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
//			for (int time = 0; time < DATA_LEN; time++) {
//				if (partId < NUM_PARTITIONS / 3) {
//					if (time < oneThird) {
//						DATA[time][partId] = 1.0;
//					} else if (time < twoThird) {
//						int diff = time - oneThird;
//						DATA[time][partId] = 1.0 - 0.8 * diff / oneThird;
//					} else {
//						DATA[time][partId] = 0.2;
//					}
//				} else if (partId >= NUM_PARTITIONS / 3 && partId < NUM_PARTITIONS * 2 / 3) {
//					if (time < oneThird) {
//						DATA[time][partId] = 0.2 + 0.8 * time / oneThird;
//					} else if (time < twoThird) {
//						DATA[time][partId] = 1.0;
//					} else {
//						int diff = time - oneThird * 2;
//						DATA[time][partId] = 1.0 - 0.8 * diff / oneThird;
//					}
//				} else {
//					if (time < oneThird) {
//						DATA[time][partId] = 0.2;
//					} else if (time < twoThird) {
//						int diff = time - oneThird;
//						DATA[time][partId] = 0.2 + 0.8 * diff / oneThird;
//					} else {
//						DATA[time][partId] = 1.0;
//					}
//				}
//			}
//		}

		STATIC_GEN_FOR_PART = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.RECORD_PER_PART, SKEW_PARAMETER));
		STATIC_GLOBAL_GEN = new AtomicReference<TwoSidedSkewGenerator>(new TwoSidedSkewGenerator(DATA_SIZE, 0.9));

		new PeriodicalJob(2000, BenchmarkerParameters.BENCHMARK_INTERVAL, new Runnable() {

			boolean notifyReplayStart, notifyReplayEnd;

			@Override
			public void run() {
				long startTime = globalStartTime.get();

				if (startTime == -1)
					return;

				long time = System.currentTimeMillis() - startTime;
				long pt = time - WARMUP_TIME;
				int timePoint = (int) (pt / (REPLAY_PREIOD / DATA_LEN));

				if (pt > 0 && timePoint >= 0 && timePoint < DATA_LEN) {
					// System.out.println(String.format("Replay Point: %d,
					// Distribution: %s",
					// timePoint, Arrays.toString(DATA[timePoint])));
					System.out.println(String.format("Current Time: %d, Replay Point: %d", time, timePoint));

					if (!notifyReplayStart) {
						System.out.println("Replay starts at " + time);
						System.out.println("Estimated time point: " + timePoint);
						notifyReplayStart = true;
					}
				} else {
					System.out.println(String.format("Current Time: %d, Replay offset: %d", time, pt));

					if (notifyReplayStart && !notifyReplayEnd) {
						System.out.println("Replay ends at " + time);
						notifyReplayEnd = true;
					}
				}
			}
		}).start();

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

	private YcsbLatestGenerator[] distributionInPart = new YcsbLatestGenerator[NUM_PARTITIONS];
	private TwoSidedSkewGenerator globalDistribution;
	private long startTime = -1;

	public GoogleWorkloadsParamGen(int nodeId) {
		GoogleWorkloadsParamGen.nodeId = nodeId;
		for (int i = 0; i < NUM_PARTITIONS; i++) {
			distributionInPart[i] = new YcsbLatestGenerator(STATIC_GEN_FOR_PART.get());
		}
		globalDistribution = new TwoSidedSkewGenerator(STATIC_GLOBAL_GEN.get());
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
			// mainPartition = genDistributionOfPart(timePoint, rvg);
			mainPartition = rvg.randomChooseFromDistribution(DATA[timePoint]);
			// System.out.println("pt " + timePoint);

			// if (!notifyReplayStart) {
			// long currentTime = (System.nanoTime() -
			// Benchmarker.BENCH_START_TIME) / 1_000_000_000;
			// System.out.println("Replay starts at " + currentTime);
			// System.out.println("Estimated time point: " + timePoint);
			// notifyReplayStart = true;
			// }
		} else { // Non-replay time
			mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
			// System.out.println("Choose " + mainPartition);

			// if (notifyReplayStart && !notifyReplayEnd) {
			// long currentTime = (System.nanoTime() -
			// Benchmarker.BENCH_START_TIME) / 1_000_000_000;
			// System.out.println("Replay ends at " + currentTime);
			// notifyReplayEnd = true;
			// }
		}

		// =====================
		// Generating Parameters
		// =====================

		int localReadCount = TOTAL_READ_COUNT;

		if (isDistributedTx) {
			localReadCount -= REMOTE_READ_COUNT;
		}

		// Read count
		paramList.add(TOTAL_READ_COUNT);

		// Read ids (in integer)
		Set<Integer> chosenIds = new HashSet<Integer>();
		for (int i = 0; i < localReadCount; i++) {
			int id = chooseARecordInMainPartition(mainPartition);
			while (!chosenIds.add(id))
				id = chooseARecordInMainPartition(mainPartition);
		}

		if (isDistributedTx) {
			int center = DATA_SIZE / 2;
			if (timePoint >= 0 && timePoint < DATA_LEN) {
				center = (timePoint + 1) * DATA_SIZE / DATA_LEN;
			}

			for (int i = 0; i < REMOTE_READ_COUNT; i++) {
				// Method 1: Choose a remote partition, then choose records in
				// it
				// int remotePartition =
				// randomChooseOtherPartition(mainPartition, rvg);
				// int id = chooseARecordInMainPartition(remotePartition);
				// while (!chosenIds.add(id))
				// id = chooseARecordInMainPartition(remotePartition);

				// Method 2: Use a global Zipfian distribution to select records
				int id = chooseARecordGlobally(center);
				while (!chosenIds.add(id))
					id = chooseARecordGlobally(center);
			}
		}

		// Add the ids to the param list
		for (Integer id : chosenIds)
			paramList.add(id);

		if (isReadWriteTx) {

			// Write count
			paramList.add(TOTAL_READ_COUNT);

			// Write ids (in integer)
			for (Integer id : chosenIds)
				paramList.add(id);

			// Write values
			for (int i = 0; i < TOTAL_READ_COUNT; i++)
				paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));

		} else {
			// Write count
			paramList.add(0);
		}

		// Insert count
		paramList.add(0);

		return paramList.toArray(new Object[0]);
	}

	private int randomChooseOtherPartition(int mainPartition, TpccValueGenerator rvg) {
		return ((mainPartition + rvg.number(1, NUM_PARTITIONS - 1)) % NUM_PARTITIONS);
	}

	// XXX: We should use long
	private int chooseARecordInMainPartition(int mainPartition) {
		int partitionStartId = getStartId(mainPartition);

		return (int) distributionInPart[mainPartition].nextValue() + partitionStartId - 1;
	}

	// XXX: We should use long
	private int chooseARecordGlobally(int center) {
		int id = (int) globalDistribution.nextValue(center) - 1;
		int partId = id / ElasqlYcsbConstants.RECORD_PER_PART;
		int offset = id % ElasqlYcsbConstants.RECORD_PER_PART;
		return partId * ElasqlYcsbConstants.MAX_RECORD_PER_PART + offset + 1;
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
