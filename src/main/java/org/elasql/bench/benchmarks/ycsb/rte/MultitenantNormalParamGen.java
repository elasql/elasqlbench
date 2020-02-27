package org.elasql.bench.benchmarks.ycsb.rte;

import java.util.ArrayList;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
import org.vanilladb.bench.benchmarks.ycsb.rte.YcsbLatestGenerator;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;

/**
 * Parameter format: [2, read count, (read id array), write count,
 * (write id array), (write value array), insert count,
 * (insert id array), (insert value array)], id = [tenant id, record id]
 * 
 * @author SLMT
 *
 */
public class MultitenantNormalParamGen implements TxParamGenerator<YcsbTransactionType> {
	
	private static final double RW_TX_RATE = ElasqlYcsbConstants.RW_TX_RATE;
	private static final int TOTAL_RECORD_COUNT = ElasqlYcsbConstants.TX_RECORD_COUNT;
	private static final int INSERT_COUNT = ElasqlYcsbConstants.ADD_INSERT_IN_WRITE_TX;
	
	private static final YcsbLatestGenerator[] LATEST_GENS;
	
	static {
		int recordPerTenant = ElasqlYcsbConstants.INIT_RECORD_PER_PART /
				ElasqlYcsbConstants.TENANTS_PER_PART;
		int tenantCount = PartitionMetaMgr.NUM_PARTITIONS *
				ElasqlYcsbConstants.TENANTS_PER_PART;
		LATEST_GENS = new YcsbLatestGenerator[tenantCount];
		for (int i = 0; i < tenantCount; i++) {
			LATEST_GENS[i] = new YcsbLatestGenerator(recordPerTenant,
					ElasqlYcsbConstants.ZIPFIAN_PARAMETER);
		}
	}
	
	private int tenantId;
	private YcsbLatestGenerator generator; 
	
	public MultitenantNormalParamGen(int tenantId) {
		this.tenantId = tenantId;
		this.generator = LATEST_GENS[tenantId];
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

		// Generate parameters
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(2); // dbtype = 2 (multi-tenants)
		
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
			paramList.add(INSERT_COUNT);
			
			// Insert ids
			for (int i = 0; i < INSERT_COUNT; i++) {
				paramList.add(tenantId);
				paramList.add((int) generator.generateInsertId());
			}
			
			// Insert values
			for (int i = 0; i < INSERT_COUNT; i++)
				paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
		} else {
			// Write count
			paramList.add(0);
			// Insert count
			paramList.add(0);
		}
		
		return paramList.toArray(new Object[paramList.size()]);
	}
}
