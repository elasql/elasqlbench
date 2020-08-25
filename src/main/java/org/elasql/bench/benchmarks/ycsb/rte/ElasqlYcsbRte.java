package org.elasql.bench.benchmarks.ycsb.rte;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
import org.vanilladb.bench.benchmarks.ycsb.rte.YcsbTxExecutor;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;
import org.vanilladb.bench.rte.TxParamGenerator;

public class ElasqlYcsbRte extends RemoteTerminalEmulator<YcsbTransactionType> {
	
	private YcsbTxExecutor executor;
	
	public ElasqlYcsbRte(SutConnection conn, StatisticMgr statMgr, int nodeId, int rteId) {
		super(conn, statMgr);
		executor = new YcsbTxExecutor(getParamGen(nodeId, rteId));
	}
	
	@Override
	protected YcsbTransactionType getNextTxType() {
		return YcsbTransactionType.YCSB;
	}

	@Override
	protected TransactionExecutor<YcsbTransactionType> getTxExeutor(YcsbTransactionType type) {
		return executor;
	}
	
	private TxParamGenerator<YcsbTransactionType> getParamGen(int nodeId, int rteId) {
		// TODO: May change due to scaling-out and consolidation
		int numOfPartitions = PartitionMetaMgr.NUM_PARTITIONS;
		
		switch (ElasqlYcsbConstants.DATABASE_MODE) {
		case SINGLE_TABLE:
			return new SingleTableNormalParamGen(numOfPartitions);
		case MULTI_TENANTS:
			int tenantId = nodeId * ElasqlYcsbConstants.TENANTS_PER_PART +
					rteId % ElasqlYcsbConstants.TENANTS_PER_PART;
			return new MultitenantNormalParamGen(tenantId);
		default:
			throw new RuntimeException("You should not be here");
		}
	}
}
