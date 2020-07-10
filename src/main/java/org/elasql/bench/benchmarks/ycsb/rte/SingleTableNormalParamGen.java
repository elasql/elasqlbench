package org.elasql.bench.benchmarks.ycsb.rte;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
import org.vanilladb.bench.benchmarks.ycsb.rte.YcsbLatestGenerator;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;

/**
 * Parameter format: [1, read count, (read id array), write count,
 * (write id array), (write value array), insert count = 0]<br/>
 * <br/>
 * Single-table does not support insertions.
 * 
 * @author SLMT
 */
public class SingleTableNormalParamGen implements TxParamGenerator<YcsbTransactionType> {
	
	private static final double RW_TX_RATE = ElasqlYcsbConstants.RW_TX_RATE;
	private static final double DIST_TX_RATE = ElasqlYcsbConstants.DIST_TX_RATE;
	private static final int TOTAL_RECORD_COUNT = ElasqlYcsbConstants.TX_RECORD_COUNT;
	private static final int REMOTE_RECORD_COUNT = ElasqlYcsbConstants.REMOTE_RECORD_COUNT;
	
	private static final AtomicReference<YcsbLatestGenerator> GEN_TEMPLATE;
	
	static {
//		int recordPerTenant = ElasqlYcsbConstants.INIT_RECORD_PER_PART /
//				ElasqlYcsbConstants.TENANTS_PER_PART;
//		int tenantCount = PartitionMetaMgr.NUM_PARTITIONS *
//				ElasqlYcsbConstants.TENANTS_PER_PART;
		GEN_TEMPLATE = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.INIT_RECORD_PER_PART,
						ElasqlYcsbConstants.ZIPFIAN_PARAMETER));
	}
	
	private int numOfPartitions;
	private YcsbLatestGenerator[] distributionInPart;
	
	public SingleTableNormalParamGen(int numOfPartitions) {
		this.numOfPartitions = numOfPartitions;
		this.distributionInPart = new YcsbLatestGenerator[numOfPartitions];
		for (int i = 0; i < numOfPartitions; i++)
			this.distributionInPart[i] = new YcsbLatestGenerator(GEN_TEMPLATE.get());
	}

	@Override
	public YcsbTransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();

		// Decide the types of transactions
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0);
		boolean isDistTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0);

		// Generate parameters
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(1); // dbtype = 1 (single-table)
		
		// Read count
		paramList.add(TOTAL_RECORD_COUNT);
		
		// Read ids
		ArrayList<Integer> ids = new ArrayList<Integer>();
		for (int i = 0; i < TOTAL_RECORD_COUNT; i++) {
			Integer id = (int) generator.generateReadId();
			while (ids.contains(id))
				id = (int) generator.generateReadId();
			paramList.add(tenantId);
			paramList.add(id);
			ids.add(id);
		}
		
		if (isReadWriteTx) {
			// Write count
			paramList.add(TOTAL_RECORD_COUNT);
			
			// Write ids
			for (Integer id : ids) {
				paramList.add(tenantId);
				paramList.add(id);
			}
			
			// Write values
			for (int i = 0; i < TOTAL_RECORD_COUNT; i++)
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
	
	private int chooseARecordInPart(int partId) {
		
	}
}
