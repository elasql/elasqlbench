package org.elasql.bench.benchmarks.tpce;

import java.util.concurrent.atomic.AtomicLong;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.tpce.TpceConstants;
import org.vanilladb.bench.benchmarks.tpce.data.TpceDataManager;

public class ElasqlTpceDataManager extends TpceDataManager {
	
	private final AtomicLong nextTradeId = new AtomicLong(0);
	
	// Scale according the number of machines
	private final int nodeId;
	
	public ElasqlTpceDataManager(int nodeId) {
		super(TpceConstants.CUSTOMER_COUNT * PartitionMetaMgr.NUM_PARTITIONS,
				TpceConstants.COMPANY_COUNT * PartitionMetaMgr.NUM_PARTITIONS,
				TpceConstants.SECURITY_COUNT * PartitionMetaMgr.NUM_PARTITIONS);
		this.nodeId = nodeId;
	}
	
	public long getNextTradeId() {
		// XXX: If we can get the number of client nodes, we can use another way to generate
		return nextTradeId.getAndIncrement() + nodeId * 100_000_000;
	}

}