package org.elasql.bench.tpcc;

import org.elasql.bench.rte.tpcc.ElasqlTpccRte;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.tpcc.TpccBenchmarker;

public class ElasqlTpccBenchmarker extends TpccBenchmarker {
	
	private int nodeId;

	private int startWid;
	private int nextWidOffset = 0;
	
	private int hotspotRte = 0;
	
	public ElasqlTpccBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver, "" + nodeId);
		this.startWid = nodeId * ElasqlTpccConstants.WAREHOUSE_PER_NODE;
		this.nodeId = nodeId;
	}
	
	@Override
	protected RemoteTerminalEmulator createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of TpccRte here
		// Note: We assume that there is a client process for each server process.
		RemoteTerminalEmulator rte = new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset + 1);
		nextWidOffset = (nextWidOffset + 1) % ElasqlTpccConstants.WAREHOUSE_PER_NODE;
		return rte;
		
		// Hotspot workloads
		// Idea: Make each client pin some RTEs on one of warehouses on server 0.
		// Implementation:
		// - Client 0 distributes its RTEs evenly to warehouses of server 0.
		// - Client X (except for client 0) pins its first Y RTEs to warehouse X of server 0.
		// - Y decides how hot server 0 is.
//		if (nodeId == 0) {
//			RemoteTerminalEmulator rte = new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset + 1);
//			nextWidOffset = (nextWidOffset + 1) % ElasqlTpccConstants.WAREHOUSE_PER_NODE;
//			return rte;
//		} else {
//			if (hotspotRte < 90) { // Y
//				hotspotRte++;
//				return new ElasqlTpccRte(conn, statMgr, nodeId);
//			} else {
//				RemoteTerminalEmulator rte = new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset + 1);
//				nextWidOffset = (nextWidOffset + 1) % ElasqlTpccConstants.WAREHOUSE_PER_NODE;
//				return rte;
//			}
//		}
	}
}
