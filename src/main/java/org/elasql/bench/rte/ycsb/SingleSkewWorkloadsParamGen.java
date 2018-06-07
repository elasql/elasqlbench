package org.elasql.bench.rte.ycsb;

import java.util.ArrayList;
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

public class SingleSkewWorkloadsParamGen implements TxParamGenerator {

	private static final double RW_TX_RATE;
	private static final double DIST_TX_RATE = 0.1;
	private static final double SKEW_PARAMETER;

	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;

	private static final int TOTAL_READ_COUNT = 2;
	private static final int REMOTE_READ_COUNT = 1;

	private static AtomicLong globalStartTime = new AtomicLong(-1);
	public static final long WARMUP_TIME = 60 * 1000;
	public static final long CHANGING_PERIOD = 90 * 1000;
	private static final double SKEW_RATIO = 0.8;

	private static final AtomicReference<YcsbLatestGenerator> TEMPLATE_GENERATOR;

	static {
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.0);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.0);

		TEMPLATE_GENERATOR = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.RECORD_PER_PART, SKEW_PARAMETER));

		new PeriodicalJob(3000, BenchmarkerParameters.BENCHMARK_INTERVAL, new Runnable() {
			@Override
			public void run() {
				long startTime = globalStartTime.get();

				if (startTime == -1)
					return;
				
				long currentTime = System.currentTimeMillis();
				if (currentTime > startTime + WARMUP_TIME) {
					// Find the hot spot partition
					int slotId = (int) ((currentTime - startTime - WARMUP_TIME) / CHANGING_PERIOD);
					int skewedPartition = slotId % NUM_PARTITIONS;
					System.out.println(String.format("Current Time: %d, Skewed Partition: %d", currentTime - startTime, skewedPartition));
				} else { // Non-skewed mode
					System.out.println(String.format("Current Time: %d, Replay offset: %d", currentTime - startTime, currentTime - startTime - WARMUP_TIME));
				}
			}
		}).start();
	}

	private static long getGlobalStartTime() {
		long time = globalStartTime.get();
		if (time == -1) {
			globalStartTime.compareAndSet(-1, System.currentTimeMillis());
			time = globalStartTime.get();

			// NOTE: BENCH_START_TIME indicates the launch time of entire
			// program
			System.out.println("Benchmark starts at " + 
					(System.nanoTime() - Benchmarker.BENCH_START_TIME) / 1_000_000_000);
		}
		return time;
	}

	private static int getStartId(int partitionId) {
		return partitionId * ElasqlYcsbConstants.MAX_RECORD_PER_PART + 1;
	}

	private YcsbLatestGenerator[] distributionInPart = new YcsbLatestGenerator[NUM_PARTITIONS];
	private long startTime = -1;

	public SingleSkewWorkloadsParamGen(int nodeId) {
		for (int i = 0; i < NUM_PARTITIONS; i++)
			distributionInPart[i] = new YcsbLatestGenerator(TEMPLATE_GENERATOR.get());
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

		// Decide if there is a hot spot (skewed) partition
		long currentTime = System.currentTimeMillis();
		if (currentTime > startTime + WARMUP_TIME) {
			// Find the hot spot partition
			int slotId = (int) ((currentTime - startTime - WARMUP_TIME) / CHANGING_PERIOD);
			int skewedPartition = slotId % NUM_PARTITIONS;

			// Choose a partition
			if (rvg.nextDouble() > SKEW_RATIO) {
				mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
				while (mainPartition == skewedPartition)
					mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
			} else {
				mainPartition = skewedPartition;
			}
		} else { // Non-skewed mode
			// Pick a partition uniformly
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
			int id = chooseARecordInMainPartition(mainPartition);
			while (!chosenIds.add(id))
				id = chooseARecordInMainPartition(mainPartition);
		}

		if (isDistributedTx) {
			for (int i = 0; i < REMOTE_READ_COUNT; i++) {
				// Choose a remote partition, then choose records in it
				int remotePartition = randomChooseOtherPartition(mainPartition, rvg);
				int id = chooseARecordInMainPartition(remotePartition);
				while (!chosenIds.add(id))
					id = chooseARecordInMainPartition(remotePartition);
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
}