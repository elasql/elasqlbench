/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.benchmarks.tpcc.rte.ElasqlTpccRte;
import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.bench.server.metadata.migration.TpccBeforePartPlan;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccBenchmarker;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ElasqlTpccBenchmarker extends TpccBenchmarker {
	
	public static final boolean ENABLE_MIGRATION_TEST = true;
	
	// For migration test
	private static final int RTE_PER_NORMAL_WAREHOUSE = 5;
	private static final int RTE_PER_HOT_WAREHOUSE = 50;
	private static final int TOTAL_RETS_FOR_NORMALS_PER_NODE = RTE_PER_NORMAL_WAREHOUSE * 
			TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART;
	private static final int NUM_OF_NORMAL_WAREHOUSE = TpccBeforePartPlan.MAX_NORMAL_WID;
	private static final int NUM_OF_HOT_WAREHOUSES = TpccBeforePartPlan.NUM_HOT_PARTS *
			TpccBeforePartPlan.HOT_WAREHOUSE_PER_HOT_PART;
	
	private int nodeId;
	private int nextWid = 0;
	private int nextRteId = 0;
	
	private static final TpccPartitionPlan partPlan = 
			ENABLE_MIGRATION_TEST? new TpccBeforePartPlan() : new TpccPartitionPlan();
	
	public static TpccPartitionPlan getPartitionPlan() {
		return partPlan;
	}
	
	public static int getNumOfWarehouses() {
		return partPlan.numOfWarehouses();
	}
	
	public int getNumOfRTEs() {
		if (ENABLE_MIGRATION_TEST) {
			if (nodeId < NUM_OF_HOT_WAREHOUSES)
				return TOTAL_RETS_FOR_NORMALS_PER_NODE + RTE_PER_HOT_WAREHOUSE;
			else
				return TOTAL_RETS_FOR_NORMALS_PER_NODE;
		} else
			return BenchmarkerParameters.NUM_RTES;
	}
	
	public ElasqlTpccBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver, Integer.toString(nodeId));
		this.nodeId = nodeId;
	}
	
	@Override
	protected RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		if (ENABLE_MIGRATION_TEST) {
			int warehouseId;
			int districtId;
			if (nextRteId < TOTAL_RETS_FOR_NORMALS_PER_NODE) { // for normal warehouses
				warehouseId = nextRteId / RTE_PER_NORMAL_WAREHOUSE +
						TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART * nodeId + 1;
				districtId = nextRteId % RTE_PER_NORMAL_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
			} else { // for hot warehouses
				int offset = nextRteId - TOTAL_RETS_FOR_NORMALS_PER_NODE;
				warehouseId = offset / RTE_PER_HOT_WAREHOUSE + NUM_OF_NORMAL_WAREHOUSE + nodeId + 1;
				districtId = offset % RTE_PER_HOT_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
			}
			nextRteId++;
			return new ElasqlTpccRte(conn, statMgr, warehouseId, districtId);
		} else {
			// NOTE: We use a customized version of TpccRte here
			ElasqlTpccRte rte = new ElasqlTpccRte(conn, statMgr, nextWid / 10 + 1, nextWid % 10 + 1);
			nextWid = (nextWid + 1) % TpccConstants.NUM_WAREHOUSES;
			return rte;
		}
	}
}
