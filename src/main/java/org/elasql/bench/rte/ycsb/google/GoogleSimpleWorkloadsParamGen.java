package org.elasql.bench.rte.ycsb.google;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.elasql.bench.rte.ycsb.ElasqlYcsbParamGen;
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

public class GoogleSimpleWorkloadsParamGen implements TxParamGenerator {

	private static final double RW_TX_RATE;
	private static final double DIST_TX_RATE;
	private static final double SKEW_PARAMETER;
	
	private static final int TOTAL_READ_COUNT = 2;
	private static final int REMOTE_READ_COUNT = 1;
	
	private static final int NUM_PARTITIONS = (MigrationMgr.ENABLE_NODE_SCALING && MigrationMgr.IS_SCALING_OUT)?
			PartitionMetaMgr.NUM_PARTITIONS - 1: PartitionMetaMgr.NUM_PARTITIONS;
	
	// Google workloads
	private static final String DATA_PATH = "/opt/shared/google-workloads.csv";
	private static final int DATA_LEN = 1440;
	private static final double DATA[][]
			= new double[DATA_LEN][NUM_PARTITIONS]; // [Time][Partition]

	public static final long WARMUP_TIME = 90_000;
	
	private static AtomicLong globalStartTime = new AtomicLong(-1);
	
	private static int nodeId;
	
	private static final AtomicReference<YcsbLatestGenerator> STATIC_GEN_FOR_PART;
	
	static {
		DIST_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".DIST_TX_RATE", 0.5);
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.5);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.0);
		STATIC_GEN_FOR_PART = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.RECORD_PER_PART, SKEW_PARAMETER));
		
		// Get data from Google Cluster
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
	private long startTime = -1;
	
	public GoogleSimpleWorkloadsParamGen(int nodeId) {
		GoogleSimpleWorkloadsParamGen.nodeId = nodeId;
		for (int i = 0; i < NUM_PARTITIONS; i++)
			distributionInPart[i] = new YcsbLatestGenerator(STATIC_GEN_FOR_PART.get());
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
		int timePoint = (int) (pt / 1000);

		// Replay time
		if (pt > 0 && timePoint >= 0 && timePoint < DATA_LEN) {
			mainPartition = rvg.randomChooseFromDistribution(DATA[timePoint]);
		} else { // Non-replay time
			mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
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
			int id = chooseARecord(mainPartition);
			while (!chosenIds.add(id))
				id = chooseARecord(mainPartition);
		}

		if (isDistributedTx) {
			// Choose a remote partition
			int remotePartition = mainPartition;
			while (remotePartition == mainPartition) {
				// Replay time
				if (pt > 0 && timePoint >= 0 && timePoint < DATA_LEN) {
					remotePartition = rvg.randomChooseFromDistribution(DATA[timePoint]);
				} else { // Non-replay time
					remotePartition = rvg.number(0, NUM_PARTITIONS - 1);
				}
			}
			
			for (int i = 0; i < REMOTE_READ_COUNT; i++) {
				int id = chooseARecord(remotePartition);
				while (!chosenIds.add(id))
					id = chooseARecord(remotePartition);
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
	
	private int chooseARecord(int partition) {
		int partitionStartId = getStartId(partition);

		return (int) distributionInPart[partition].nextValue() + partitionStartId - 1;
	}
}
