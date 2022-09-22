package org.elasql.bench.benchmarks.tpcc.rte;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccBenchmark;
import org.elasql.bench.benchmarks.tpcc.ElasqlTpccConstants;
import org.elasql.bench.server.metadata.migration.TpccBeforePartPlan;
import org.elasql.bench.workloads.GoogleWorkload;
import org.elasql.bench.workloads.MultiTrendHotPartitionWorkload;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccValueGenerator;

public class WarehouseSelector {
	
	private static final int WID_CHANGE_PERIOD_MS = 25;
	// 0: standard, 1: time dependent, 2: hybrid (2 hotspot in a window), 3: hybrid, 4: dynamic, 5: Google
	private static final int WORKLOAD_TYPE = ElasqlTpccRte.TYPE;
	private static final double ORIGINAL_RTE_PERCENTAGE = 0.5; // for type 2
	private static final double SKEW_RATIO = 0.8;
	
	private static MultiTrendHotPartitionWorkload hotPartWorkload = null;
	private static GoogleWorkload googleWorkload = null;
	
	private static final AtomicLong GLOBAL_START_TIME = new AtomicLong(0);
	
	static {
		switch (WORKLOAD_TYPE) {
		case 4:
			hotPartWorkload = new MultiTrendHotPartitionWorkload();
			break;
		case 5:
			googleWorkload = new GoogleWorkload(1_000);
			break;
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
			return (int) (System.currentTimeMillis() / WID_CHANGE_PERIOD_MS % numOfWarehouses) + 1; 
		case 2: // skewness must > 0
			if (random.nextDouble() < ORIGINAL_RTE_PERCENTAGE) 
				return defaultHomeWid;
			else {
				int wid = defaultHomeWid % TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART;
				int nodeId = (int) (System.currentTimeMillis() / WID_CHANGE_PERIOD_MS % PartitionMetaMgr.NUM_PARTITIONS);
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
		case 4: // Hot Partition
			partId = hotPartWorkload.getShortTermFocusedPart(elapsedTime);
			return selectWarehouseInPart(partId);
		case 5: // Google
			partId = googleWorkload.randomlySelectPartId(elapsedTime);
			return selectWarehouseInPart(partId);
		default: 
			throw new UnsupportedOperationException(); 
		}
	}
	
	public int getRemoteWid() {
		int partId;
		long elapsedTime = getElapsedTimeMs();
		int remoteWid = defaultHomeWid;
		
		switch (WORKLOAD_TYPE) {
		case 4: // Hot Partition
			while (remoteWid == defaultHomeWid) {
				partId = hotPartWorkload.randomlySelectPartId(elapsedTime);
				remoteWid = selectWarehouseInPart(partId);
			}
			return remoteWid;
		case 5: // Google
			while (remoteWid == defaultHomeWid) {
				partId = googleWorkload.randomlySelectPartId(elapsedTime);
				remoteWid = selectWarehouseInPart(partId);
			}
			return remoteWid;
		default: 
			return valueGen.numberExcluding(1, numOfWarehouses, defaultHomeWid);
		}
	}
	
	private int selectWarehouseInPart(int partId) {
		int startWid = partId * ElasqlTpccConstants.WAREHOUSE_PER_PART + 1;
		int widOffset = random.nextInt(ElasqlTpccConstants.WAREHOUSE_PER_PART);
		return (startWid + widOffset);
	}

}
