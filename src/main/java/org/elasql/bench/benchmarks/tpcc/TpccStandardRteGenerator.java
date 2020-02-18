package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.benchmarks.tpcc.rte.ElasqlTpccRte;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class TpccStandardRteGenerator implements TpccRteGenerator {
	
	private int startWid;
	private int nextWidOffset = 0, nextDid = 1;
	
	public TpccStandardRteGenerator(int nodeId) {
		startWid = nodeId * ElasqlTpccConstants.WAREHOUSE_PER_PART + 1;
	}
	
	@Override
	public int getNumOfRTEs() {
		return BenchmarkerParameters.NUM_RTES;
	}

	@Override
	public RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of TpccRte here
		ElasqlTpccRte rte = new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset, nextDid);
		
		// Find the next ids
		nextDid++;
		if (nextDid > TpccConstants.DISTRICTS_PER_WAREHOUSE) {
			nextDid = 1;
			nextWidOffset++;
			if (nextWidOffset >= ElasqlTpccConstants.WAREHOUSE_PER_PART) {
				nextWidOffset = 0;
			}
		}
		
		return rte;
	}
}
