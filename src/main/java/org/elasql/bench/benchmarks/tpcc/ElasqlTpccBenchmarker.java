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
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccBenchmarker;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ElasqlTpccBenchmarker extends TpccBenchmarker {
	
	public static final boolean ENABLE_MIGRATION_TEST = true;
	
	private int nextWid = 0;
	
	private static final TpccPartitionPlan partPlan = 
			ENABLE_MIGRATION_TEST? new TpccBeforePartPlan() : new TpccPartitionPlan();
	
	public static TpccPartitionPlan getPartitionPlan() {
		return partPlan;
	}
	
	public static int getNumOfWarehouses() {
		return partPlan.numOfWarehouses();
	}
	
	public ElasqlTpccBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver, Integer.toString(nodeId));
	}
	
	@Override
	protected RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of TpccRte here
		ElasqlTpccRte rte = new ElasqlTpccRte(conn, statMgr, nextWid / 10 + 1, nextWid % 10 + 1);
		// TODO: Add for migrations
		nextWid = (nextWid + 1) % TpccConstants.NUM_WAREHOUSES;
		return rte;
	}
}
