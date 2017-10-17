package org.elasql.bench.rte.ycsb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.tpcc.TpccValueGenerator;
import org.vanilladb.bench.util.YcsbLatestGenerator;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.bench.ycsb.YcsbTransactionType;

public class ElasqlYcsbParamGen implements TxParamGenerator {
	
	private static final double RW_TX_RATE;
	private static final double SKEW_PARAMETER;
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	private static final AtomicInteger[] GLOBAL_COUNTERS;
	
	private static int nodeId;
	private YcsbLatestGenerator[] latestRandoms = new YcsbLatestGenerator[NUM_PARTITIONS];
	private YcsbLatestGenerator latestRandom;
	
	
	static {
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.0);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.0);
		
		System.out.println("Parameters:");
		System.out.println("Read Write Tx Rate: " + RW_TX_RATE);
		System.out.println("Skew Parameter: " + SKEW_PARAMETER);
	}
	
	static {
		if (NUM_PARTITIONS == -1)
			throw new RuntimeException("it's -1 !!!!");
		
		GLOBAL_COUNTERS = new AtomicInteger[NUM_PARTITIONS];
		for (int i = 0; i < NUM_PARTITIONS; i++)
			GLOBAL_COUNTERS[i] = new AtomicInteger(0);
	}
	
	private static int getNextInsertId(int partitionId) {
		int id = GLOBAL_COUNTERS[partitionId].getAndIncrement();
		int CLIENT_COUNT = NUM_PARTITIONS;
		
		return id * CLIENT_COUNT + nodeId + getStartId(partitionId) + getRecordCount(partitionId);
	}
	
	private static int getStartId(int partitionId) {
		return partitionId * ElasqlYcsbConstants.MAX_RECORD_PER_PART + 1;
	}
	
	private static int getRecordCount(int partitionId) {
		return ElasqlYcsbConstants.RECORD_PER_PART;
	}

	public ElasqlYcsbParamGen(int nodeId) {
		ElasqlYcsbParamGen.nodeId = nodeId;
		for (int i = 0; i < NUM_PARTITIONS; i++) {
			int partitionSize = getRecordCount(i);
			latestRandoms[i] = new YcsbLatestGenerator(partitionSize, SKEW_PARAMETER);
		}
	}
	
	@Override
	public TransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}
	
	public static void main(String[] args) {
		int rteCount = 100;
		int eachRun = 10;
		ElasqlYcsbParamGen[] rtes = new ElasqlYcsbParamGen[rteCount];
		
		// Create RTEs
		for (int i = 0; i < rteCount; i++) {
			rtes[i] = new ElasqlYcsbParamGen(i);
		}
		
		// Generate parameters
		for (int i = 0; i < eachRun; i++) {
				System.out.println(Arrays.toString(rtes[0].generateParameter()));
		}	
	}

	@Override
	public Object[] generateParameter() {
		TpccValueGenerator rvg = new TpccValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();
		
		// ================================
		// Decide the types of transactions
		// ================================
		
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;
		
//		if (NUM_PARTITIONS < 2)
//			isDistributedTx = false;
		
		// =========================================
		// Decide the counts and the main partitions
		// =========================================

		// Choose the main partition
		int mainPartition = 0;
		
		mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
		latestRandom = latestRandoms[mainPartition];
		
		// =====================
		// Generating Parameters
		// =====================
		
		if (isReadWriteTx) {
			int readWriteId = chooseARecordInMainPartition(mainPartition);
			int insertId = getNextInsertId(mainPartition);
			
			// Read count
			paramList.add(1);
			
			// Read ids (in integer)
			paramList.add(readWriteId);
			
			// Write count
			paramList.add(1);
			
			// Write ids (in integer)
			paramList.add(readWriteId);
			
			// Write values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
			// Insert count
			paramList.add(1);
			
			// Insert ids (in integer)
			paramList.add(insertId);
			
			// Insert values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
		} else {
			int rec1Id = chooseARecordInMainPartition(mainPartition);
			int rec2Id = rec1Id;
			while (rec1Id == rec2Id)
				rec2Id = chooseARecordInMainPartition(mainPartition);
			
			// Read count
			paramList.add(2);
			
			// Read ids (in integer)
			paramList.add(rec1Id);
			paramList.add(rec2Id);
			
			// Write count
			paramList.add(0);
			
			// Insert count
			paramList.add(0);
		}
		
		return paramList.toArray(new Object[0]);
	}
	
	private int chooseARecordInMainPartition(int mainPartition) {
		int partitionStartId = getStartId(mainPartition);
		
		return (int) latestRandom.nextValue() + partitionStartId - 1;
	}
}
