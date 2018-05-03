package org.elasql.bench.rte.ycsb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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

public class GoogleMultitanentParamGen implements TxParamGenerator {
	
	private static final double RW_TX_RATE;
	private static final double SKEW_PARAMETER;
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	private static final int TANENTS_PER_PART = 4;
	private static final int NUM_TANENTS = NUM_PARTITIONS * TANENTS_PER_PART;
	private static final int RECORD_PER_TANENT = ElasqlYcsbConstants.RECORD_PER_PART / TANENTS_PER_PART;
	
	private static final int TOTAL_READ_COUNT = 2;

	// Real parameter
	private static final int DATA_LEN = 51;
	private static double DATA[][] = new double[DATA_LEN][NUM_TANENTS]; // [Time][Tanent Id]

	private static AtomicLong globalStartTime = new AtomicLong(-1);
	private static final long REPLAY_PREIOD;
	private static final long WARMUP_TIME;

	private static final AtomicReference<YcsbLatestGenerator> STATIC_GEN_FOR_TANENT;

	static {
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.0);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.99);

		WARMUP_TIME = 90 * 1000;
		REPLAY_PREIOD = 153 * 1000;

		// Get data from Google Cluster
		// Directly choose 1 ~ NUM_PARTITIONS workloads
//		int target[] = new int[NUM_TANENTS];
//		for (int i = 0; i < NUM_TANENTS; i++)
//			target[i] = i + 1;
		// Assign the chosen workloads (for 20 nodes)
//		int target[] = {
//			9768, 8962, 4179, 12070, 6737, 4509, 11475, 11898, 11384, 4900, // Former Skews
//			3165, 7733, 1359, 9572, 1958, 5038, 12122, 10304, 316, 4019, // Later Skews
//			 // Stables
//		};
		// Assign the chosen workloads (for 8 nodes)
//		int target[] = {
//			11475, 11898, 11384, 4900, // Former Skews
//			3165, 7733, 1359, 9572, // Later Skews
//			 // Stables
//		};
		// Assign the chosen workloads (for 80 tanents)
		int target[] = {
			4179, 4509, 4900, 6737, 7911, 8515, 8962, 9226, 9768, 10296, // Former Skews (10)
			5469, 5767, 5968, 6406, 6491, 6581, 6991, 7032, 7100, 7813, // Low loading (10)
			311, 422, 558, 619, 805, 1083, 1721, 1963, 2310, 3012, 3399, // Low loading (10)
			400, 2202, 2356, 2384, 3584, 3744, 5615, 5972, 6145, 6320, // Middle Skews (10)
			7941, 8040, 8165, 8202, 8341, 8520, 8630, 8988, 9733, 9915, // Low loading (10)
			3481, 3530, 3532, 4164, 4337, 4344, 4493, 5242, 5272, 5391, // Low loading (10)
			316, 1359, 1958, 3165, 4019, 4284, 5038, 5073, 7733, 9020, // Later Skews (10)
			9951, 10223, 10650, 10758, 11067, 11558, 11630, 11738, 12299 // Low loading (10)
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

			while (hitCount < NUM_TANENTS && line != null) {
				// Find the target row
				hit = false;
				for (partId = 0; partId < NUM_TANENTS; partId++) {
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
		
		// Normalization
//		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
//			// Find the min and max
//			double min = Double.MAX_VALUE;
//			double max = Double.MIN_VALUE;
//			for (int i = 0; i < DATA_LEN; i++) {
//				if (min > DATA[partId][i])
//					min = DATA[partId][i];
//				if (max < DATA[partId][i])
//					max = DATA[partId][i];
//			}
//			
//			// Scale and transition
//			double scale = max - min;
//			for (int i = 0; i < DATA_LEN; i++) {
//				DATA[partId][i] = (DATA[partId][i] - min) / scale;
//			}
//			
//			// Make the odd workloads twice larger
////			if (partId % 2 == 1)
////				for (int i = 0; i < DATA_LEN; i++) {
////					DATA[partId][i] *= 2;
////				}
//			
//			// Make the min become 0.1
//			for (int i = 0; i < DATA_LEN; i++) {
//				DATA[partId][i] += 0.1;
//			}
//		}
		
		// Alter the data distribution for testing
//		int oneThird = DATA_LEN / 3;
//		int twoThird = 2 * oneThird;
//		for (int partId = 0; partId < NUM_TANENTS; partId++) {
//			for (int time = 0; time < DATA_LEN; time++) {
//				if (partId < NUM_TANENTS / 5) {
//					if (time < oneThird) {
//						DATA[time][partId] = 1.0;
//					} else if (time < twoThird) {
//						int diff = time - oneThird;
//						DATA[time][partId] = 1.0 - 0.9 * diff / oneThird;
//					} else {
//						DATA[time][partId] = 0.1;
//					}
//				} else if (partId > NUM_TANENTS * 4 / 5) {
//					if (time < oneThird) {
//						DATA[time][partId] = 0.1;
//					} else if (time < twoThird) {
//						int diff = time - oneThird;
//						DATA[time][partId] = 0.1 + 0.9 * diff / oneThird;
//					} else {
//						DATA[time][partId] = 1.0;
//					}
//				} else {
//					DATA[time][partId] = 0.1;
//				}
//			}
//		}

		STATIC_GEN_FOR_TANENT = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.RECORD_PER_PART / TANENTS_PER_PART, SKEW_PARAMETER));
		
		new PeriodicalJob(2000, BenchmarkerParameters.BENCHMARK_INTERVAL, 
			new Runnable() {
		
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
//						System.out.println(String.format("Replay Point: %d, Distribution: %s", 
//							timePoint, Arrays.toString(DATA[timePoint])));
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
			}
		).start();
	}

	private static long getGlobalStartTime() {
		long time = globalStartTime.get();
		if (time == -1) {
			globalStartTime.compareAndSet(-1, System.currentTimeMillis());
			time = globalStartTime.get();
		}
		return time;
	}
	
	public static void main(String[] args) {
		GoogleMultitanentParamGen gen = new GoogleMultitanentParamGen(0);
		for (int i = 0; i < 100; i++) {
			Object[] p = gen.generateParameter();
			System.out.println(Arrays.toString(p));
		}
	}

	private YcsbLatestGenerator[] distributionInTanent = new YcsbLatestGenerator[NUM_TANENTS];
	private long startTime = -1;

	public GoogleMultitanentParamGen(int nodeId) {
		for (int i = 0; i < NUM_TANENTS; i++) {
			distributionInTanent[i] = new YcsbLatestGenerator(STATIC_GEN_FOR_TANENT.get());
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

		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;

		/////////////////////////////

		// =========================================
		// Decide the counts and the main partitions
		// =========================================

		// Choose the main partition
		int tanentId = 0;

		long pt = (System.currentTimeMillis() - startTime) - WARMUP_TIME;
		int timePoint = (int) (pt / (REPLAY_PREIOD / DATA_LEN));

		// Replay time
		if (pt > 0 && timePoint >= 0 && timePoint < DATA_LEN) {
//			mainPartition = genDistributionOfPart(timePoint, rvg);
			tanentId = rvg.randomChooseFromDistribution(DATA[timePoint]);
			// System.out.println("pt " + timePoint);

//			if (!notifyReplayStart) {
//				long currentTime = (System.nanoTime() - Benchmarker.BENCH_START_TIME) / 1_000_000_000;
//				System.out.println("Replay starts at " + currentTime);
//				System.out.println("Estimated time point: " + timePoint);
//				notifyReplayStart = true;
//			}
		} else { // Non-replay time
			tanentId = rvg.number(0, NUM_TANENTS - 1);
			// System.out.println("Choose " + mainPartition);

//			if (notifyReplayStart && !notifyReplayEnd) {
//				long currentTime = (System.nanoTime() - Benchmarker.BENCH_START_TIME) / 1_000_000_000;
//				System.out.println("Replay ends at " + currentTime);
//				notifyReplayEnd = true;
//			}
		}

		// =====================
		// Generating Parameters
		// =====================
		
		int localReadCount = TOTAL_READ_COUNT;
		
		// Read count
		paramList.add(TOTAL_READ_COUNT);

		// Read ids (in integer)
		Set<Integer> chosenIds = new HashSet<Integer>();
		for (int i = 0; i < localReadCount; i++) {
			int id = chooseARecordInTanent(tanentId);
			while (!chosenIds.add(id))
				id = chooseARecordInTanent(tanentId);
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
	
	// XXX: We should use long
	private int chooseARecordInTanent(int tanentId) {
		int partId = tanentId / TANENTS_PER_PART;
		int tanentInPart = tanentId % TANENTS_PER_PART;
		int tanentStartId = partId * ElasqlYcsbConstants.MAX_RECORD_PER_PART + tanentInPart * RECORD_PER_TANENT;
		long offset = distributionInTanent[tanentId].nextValue();
		return (int) (tanentStartId + offset);
	}
}
