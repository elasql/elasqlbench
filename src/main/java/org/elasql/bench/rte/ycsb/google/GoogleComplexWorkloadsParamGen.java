package org.elasql.bench.rte.ycsb.google;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.elasql.bench.rte.ycsb.ElasqlYcsbParamGen;
import org.elasql.bench.rte.ycsb.TwoSidedSkewGenerator;
import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.migration.MigrationMgr;
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

public class GoogleComplexWorkloadsParamGen implements TxParamGenerator {

	private static final double RW_TX_RATE;
	private static final double DIST_TX_RATE;
	private static final double SKEW_PARAMETER;
	private static final double GLOBAL_SKEW;
	private static final int GLOBAL_SKEW_CHANGE_PERIOD = 1; // in seconds
	
	private static final int TOTAL_READ_COUNT = 2;
	private static final int REMOTE_READ_COUNT = 1;
	
	private static final boolean IS_DYNAMIC_READ_COUNT = false;
	private static final int MEAN_READ_COUNT = 20;
	private static final int STD_READ_COUNT = 10;
	
	private static final long SENDING_DELAY = ElasqlYcsbConstants.SENDING_DELAY; // ms
	
	private static final int NUM_PARTITIONS = (MigrationMgr.ENABLE_NODE_SCALING && MigrationMgr.IS_SCALING_OUT)?
			PartitionMetaMgr.NUM_PARTITIONS - 1: PartitionMetaMgr.NUM_PARTITIONS;
	private static final int DATA_SIZE = ElasqlYcsbConstants.RECORD_PER_PART * NUM_PARTITIONS;
	
	// Google workloads
	private static final String DATA_PATH = "/opt/shared/google-workloads-2min-3days.csv";
	private static final int DATA_LEN = 2160;
	private static final double DATA[][]
			= new double[DATA_LEN][NUM_PARTITIONS]; // [Time][Partition]
	private static final boolean USE_EVEN_LOAD = false;
	private static final int GLOBAL_SKEW_REPEAT = 3; // 3 runs for 3 days

	public static final long WARMUP_TIME = 90_000;
	
	private static AtomicLong globalStartTime = new AtomicLong(-1);
	
	private static int nodeId;
	
	private static final AtomicReference<YcsbLatestGenerator> STATIC_GEN_FOR_PART;
	private static final AtomicReference<TwoSidedSkewGenerator> STATIC_GLOBAL_GEN;
	
	static {
		DIST_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".DIST_TX_RATE", 0.5);
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.5);
//		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
//				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.9);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.9);
		GLOBAL_SKEW = SKEW_PARAMETER;
		STATIC_GEN_FOR_PART = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.RECORD_PER_PART, SKEW_PARAMETER));
		STATIC_GLOBAL_GEN = new AtomicReference<TwoSidedSkewGenerator>(new TwoSidedSkewGenerator(DATA_SIZE, GLOBAL_SKEW));
		
		// Get data from Google Cluster
		if (USE_EVEN_LOAD) {
			double load = 1.0 / NUM_PARTITIONS;
			for (int time = 0; time < DATA_LEN; time++)
				Arrays.fill(DATA[time], load);
		} else {
			try (BufferedReader reader = new BufferedReader(new FileReader(DATA_PATH))) {
				// Data Format: Each row is a workload of a node, each value is the
				for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
					String line = reader.readLine();
					String[] loads = line.split(",");
					for (int time = 0; time < DATA_LEN; time++) {
						DATA[time][partId] = Double.parseDouble(loads[time]);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// A logger for debug
		new PeriodicalJob(5000, BenchmarkerParameters.BENCHMARK_INTERVAL, new Runnable() {

			boolean notifyReplayStart, notifyReplayEnd;

			@Override
			public void run() {
				long startTime = globalStartTime.get();

				if (startTime == -1)
					return;

				long time = System.currentTimeMillis() - startTime;
				long pt = time - WARMUP_TIME;
				int timePoint = (int) (pt / 1000);

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
	}

	private static long getGlobalStartTime() {
		long time = globalStartTime.get();
		if (time == -1) {
			globalStartTime.compareAndSet(-1, System.currentTimeMillis());
			time = globalStartTime.get();
		}
		return time;
	}

	private static int getStartId(int partitionId) {
		return partitionId * ElasqlYcsbConstants.RECORD_PER_PART + 1;
	}

	private YcsbLatestGenerator[] distributionInPart = new YcsbLatestGenerator[NUM_PARTITIONS];
	private TwoSidedSkewGenerator globalDistribution;
	private long startTime = -1;
	
	public GoogleComplexWorkloadsParamGen(int nodeId) {
		GoogleComplexWorkloadsParamGen.nodeId = nodeId;
		for (int i = 0; i < NUM_PARTITIONS; i++)
			distributionInPart[i] = new YcsbLatestGenerator(STATIC_GEN_FOR_PART.get());
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
		
		// Delay the transaction
		if (SENDING_DELAY > 0) {
			try {
				Thread.sleep(SENDING_DELAY);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		// Check current time
		long pt = (System.currentTimeMillis() - startTime) - WARMUP_TIME;
		int timePoint = (int) (pt / 1000);

		// ================================
		// Decide the types of transactions
		// ================================

		boolean isDistributedTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0) ? true
				: false;
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;

		if (NUM_PARTITIONS < 2)
			isDistributedTx = false;
		
		// There is no distributed tx in non-replay time
		if (timePoint < 0 || timePoint > DATA_LEN) {
			isDistributedTx = false;
		}

		/////////////////////////////

		// =========================================
		// Decide the counts and the main partitions
		// =========================================

		// Choose the main partition
		int mainPartition = 0;

		// Replay time
		if (pt > 0 && timePoint >= 0 && timePoint < DATA_LEN) {
			mainPartition = rvg.randomChooseFromDistribution(DATA[timePoint]);
		} else { // Non-replay time
			mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
		}

		// =====================
		// Generating Parameters
		// =====================
		
		int totalReadCount = TOTAL_READ_COUNT;
		int remoteReadCount = REMOTE_READ_COUNT;
		
		if (IS_DYNAMIC_READ_COUNT) {
			double zeroMeanRandom = rvg.rng().nextGaussian();
			int randomCount = (int) (MEAN_READ_COUNT + zeroMeanRandom * STD_READ_COUNT);
			if (randomCount <= 1)
				randomCount = 2;
			if (randomCount > 50)
				randomCount = 50;
			
			totalReadCount = randomCount;
			remoteReadCount = randomCount / 2;
		}
		
		int localReadCount = totalReadCount;
		if (isDistributedTx) {
			localReadCount -= remoteReadCount;
		}

		// Read count
		paramList.add(totalReadCount);

		// Read ids (in integer)
		Set<Integer> chosenIds = new HashSet<Integer>();
		for (int i = 0; i < localReadCount; i++) {
			int id = chooseARecord(mainPartition);
			while (!chosenIds.add(id))
				id = chooseARecord(mainPartition);
		}

		if (isDistributedTx) {
			// Use a global Zipfian distribution to select records
			int center = DATA_SIZE / 2;
			if (timePoint >= 0 && timePoint < DATA_LEN) {
				// Note that it might be overflowed here.
				// The center of the 2-sided distribution changes
				// as the time increases. It moves from 0 to DATA_SIZE
				// and bounces back when it hits the end of the range. 
				int windowSize = DATA_LEN / GLOBAL_SKEW_REPEAT;
				int timeOffset = timePoint % (2 * windowSize);
				if (timeOffset >= windowSize)
					timeOffset = 2 * windowSize - timeOffset;
				center = DATA_SIZE / (windowSize / GLOBAL_SKEW_CHANGE_PERIOD);
				center *= (((timeOffset % windowSize) / GLOBAL_SKEW_CHANGE_PERIOD) + 1); 
			}
			
			for (int i = 0; i < remoteReadCount; i++) {
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
			paramList.add(totalReadCount);

			// Write ids (in integer)
			for (Integer id : chosenIds)
				paramList.add(id);

			// Write values
			for (int i = 0; i < totalReadCount; i++)
				paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));

		} else {
			// Write count
			paramList.add(0);
		}

		// Insert count
		paramList.add(0);

		return paramList.toArray(new Object[0]);
	}
	
//	private static int lastTimePoint;

	private int chooseARecordGlobally(int center) {
		// Original
		return (int) globalDistribution.nextValue(center);
		// Uniform chooses
//		return random.nextInt(DATA_SIZE) + 1;
	}
	
	private int chooseARecord(int partition) {
		int partitionStartId = getStartId(partition);

		return (int) distributionInPart[partition].nextValue() + partitionStartId - 1;
	}
}
