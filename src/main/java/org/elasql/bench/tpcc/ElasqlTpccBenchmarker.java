package org.elasql.bench.tpcc;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.rte.tpcc.ElasqlTpccRte;
import org.elasql.bench.util.ElasqlBenchProperties;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.tpcc.TpccBenchmarker;

public class ElasqlTpccBenchmarker extends TpccBenchmarker {
	private static Logger logger = Logger.getLogger(ElasqlTpccBenchmarker.class.getName());
	
	private static final double SKEW_RATIO;
	private static final int HOT_RTE_END_ID;
	
	static {
		SKEW_RATIO = ElasqlBenchProperties.getLoader().getPropertyAsDouble(
				ElasqlTpccBenchmarker.class.getName() + ".SKEW_RATIO", 0.0);
		HOT_RTE_END_ID = (int) (BenchmarkerParameters.NUM_RTES * SKEW_RATIO);
	}
	
	private int nodeId;

	private int startWid;
	private int nextWidOffset = 0;
	
	private int hotspotRte = 0;
	
	public ElasqlTpccBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver, "" + nodeId);
		this.startWid = nodeId * ElasqlTpccConstants.WAREHOUSE_PER_NODE;
		this.nodeId = nodeId;
		
		if (HOT_RTE_END_ID > 0 && logger.isLoggable(Level.INFO))
			logger.info("TPC-C uses hot-spot workloads (first " + HOT_RTE_END_ID + " RTEs are hot)");
	}
	
	@Override
	protected RemoteTerminalEmulator createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of TpccRte here
		// Note: We assume that there is a client process for each server process.
//		RemoteTerminalEmulator rte = new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset + 1);
//		nextWidOffset = (nextWidOffset + 1) % ElasqlTpccConstants.WAREHOUSE_PER_NODE;
//		return rte;
		
		// Hotspot workloads
		// Idea: Make each client pin some RTEs on one of warehouses on server 0.
		// Implementation:
		// - Client 0 distributes its RTEs evenly to warehouses of server 0.
		// - Client X (except for client 0) pins its first Y RTEs to warehouse X of server 0.
		// - Y decides how hot server 0 is.
		if (nodeId == 0) {
			RemoteTerminalEmulator rte = new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset + 1);
			nextWidOffset = (nextWidOffset + 1) % ElasqlTpccConstants.WAREHOUSE_PER_NODE;
			return rte;
		} else {
			if (hotspotRte < HOT_RTE_END_ID) { // Y
				hotspotRte++;
				return new ElasqlTpccRte(conn, statMgr, nodeId);
			} else {
				RemoteTerminalEmulator rte = new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset + 1);
				nextWidOffset = (nextWidOffset + 1) % ElasqlTpccConstants.WAREHOUSE_PER_NODE;
				return rte;
			}
		}
	}
}
