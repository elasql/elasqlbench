package org.elasql.bench.benchmarks.tpcc.rte;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccBenchmark;
import org.elasql.bench.benchmarks.tpcc.ElasqlTpccConstants;
import org.elasql.bench.server.metadata.migration.TpccBeforePartPlan;
import org.elasql.bench.workloads.MultiTrendGoogleWorkload;
import org.elasql.bench.workloads.MultiTrendHotPartitionWorkload;
import org.elasql.bench.workloads.MultiTrendWorkload;
import org.elasql.bench.workloads.Workload;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.benchmarks.tpcc.TpccValueGenerator;

public class WarehouseSelector {
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	// Short-term skewness
	private static final int SHORT_TERM_SKEW_LENGTH = 25; // in milliseconds
	
	private static final int WID_CHANGE_PERIOD_MS = 25;
	// 0: standard, 1: time dependent, 2: hybrid (2 hotspot in a window), 3: hybrid, 4: dynamic, 5: Google
	private static final int WORKLOAD_TYPE = ElasqlTpccRte.TYPE;
	private static final double ORIGINAL_RTE_PERCENTAGE = 0.5; // for type 2
	private static final double SKEW_RATIO = 0.8;
	
	private static final long WARMUP_TIME = 60_000; // in milliseconds
	private static final long WORKLOAD_END_TIME;
	
	private static final int GOOGLE_WINDOW_SIZE = 5_000;
	
	private static Workload workload = null;
	
	private static final AtomicLong GLOBAL_START_TIME = new AtomicLong(0);
		
	static {
		// Setup workload
		switch (WORKLOAD_TYPE) {
		case 1: // Normal Uniform Workload with short-term skewness
			int length = (int) (BenchmarkerParameters.WARM_UP_INTERVAL +
					BenchmarkerParameters.BENCHMARK_INTERVAL) / 1000 + 10;
			double[][] uniformWorkload = new double[length][NUM_PARTITIONS];
			for (int timeIdx = 0; timeIdx < length; timeIdx++) {
				Arrays.fill(uniformWorkload[timeIdx], 1.0 / NUM_PARTITIONS);
			}
			workload = new MultiTrendWorkload(uniformWorkload, 1000, SHORT_TERM_SKEW_LENGTH);
			break;
		case 4: // Hot Partition
			workload = new MultiTrendHotPartitionWorkload();
			break;
		case 5: // Google
			workload = new MultiTrendGoogleWorkload(GOOGLE_WINDOW_SIZE, 50);
			break;
		}
		
		// Setup workload end time
		switch (WORKLOAD_TYPE) {
		case 4:
		case 5:
			WORKLOAD_END_TIME = WARMUP_TIME + workload.getWorkloadLengthMs();
			break;
		default:
			WORKLOAD_END_TIME = WARMUP_TIME;
		}
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
	
	private int previousTime = -1;
	private int previosWareHouse = -1;
	
	private int defaultHomeWid;
	private int numOfWarehouses = ElasqlTpccBenchmark.getNumOfWarehouses();
	private Random random = new Random(0);
	private TpccValueGenerator valueGen = new TpccValueGenerator();
	
	public WarehouseSelector(int defaultHomeWid) {
		this.defaultHomeWid = defaultHomeWid;
	}
	
	public int getHomeWid() {
		int partId;
		long elapsedTime = getElapsedTimeMs();
		
		switch (WORKLOAD_TYPE) { 
		case 0: 
			return defaultHomeWid; 
		case 1:
			partId = workload.selectMainPartition(elapsedTime);
			return selectWarehouseInPart(partId);
		case 2: // skewness must > 0
			if (random.nextDouble() < ORIGINAL_RTE_PERCENTAGE) 
				return defaultHomeWid;
			else {
				int wid = defaultHomeWid % TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART;
				int nodeId = (int) (System.currentTimeMillis() / WID_CHANGE_PERIOD_MS % NUM_PARTITIONS);
				return wid + nodeId * TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART + 1; 
			}			
		case 3: // skewness must > 0
			int currentWareHouse = (int) (System.currentTimeMillis() / WID_CHANGE_PERIOD_MS % numOfWarehouses) + 1;
			if (currentWareHouse != previousTime) {
				previousTime = currentWareHouse;
				if (random.nextDouble() < SKEW_RATIO) {
					previosWareHouse = defaultHomeWid;
				} else {
					previosWareHouse = currentWareHouse;
				}
			}
			return previosWareHouse;
		case 4:
		case 5:
			if (elapsedTime > WARMUP_TIME && elapsedTime < WORKLOAD_END_TIME)
				partId = workload.selectMainPartition(elapsedTime - WARMUP_TIME);
			else
				partId = valueGen.number(0, NUM_PARTITIONS - 1);
			return selectWarehouseInPart(partId);
		default: 
			throw new UnsupportedOperationException(); 
		}
	}
	
	public int getRemoteWid(int homeWid) {
		int partId;
		long elapsedTime = getElapsedTimeMs();
		int remoteWid = homeWid;
		
		switch (WORKLOAD_TYPE) {
		case 4:
		case 5:
			while (remoteWid == homeWid) {
				if (elapsedTime > WARMUP_TIME && elapsedTime < WORKLOAD_END_TIME)
					partId = workload.selectRemotePartition(elapsedTime - WARMUP_TIME);
				else
					partId = valueGen.number(0, NUM_PARTITIONS - 1);
				remoteWid = selectWarehouseInPart(partId);
			}
			return remoteWid;
		default: 
			return valueGen.numberExcluding(1, numOfWarehouses, homeWid);
		}
	}
	
	private int selectWarehouseInPart(int partId) {
		int startWid = partId * ElasqlTpccConstants.WAREHOUSE_PER_PART + 1;
		int widOffset = random.nextInt(ElasqlTpccConstants.WAREHOUSE_PER_PART);
		return (startWid + widOffset);
	}

}
