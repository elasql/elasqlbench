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
package org.elasql.bench.benchmarks.recon;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.vanilladb.bench.BenchTransactionType;
import org.vanilladb.bench.Benchmark;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ReconBenchmark extends Benchmark {

	@Override
	public Set<BenchTransactionType> getBenchmarkingTxTypes() {
		Set<BenchTransactionType> txTypes = new HashSet<BenchTransactionType>();
		for (ReconbenchTransactionType txType : ReconbenchTransactionType.values()) {
			if (txType.isBenchmarkingProcedure())
				txTypes.add(txType);
		}
		return txTypes;
	}

	@Override
	public void executeLoadingProcedure(SutConnection conn) throws SQLException {
		conn.callStoredProc(ReconbenchTransactionType.TESTBED_LOADER.getProcedureId(), new Object[] {});
	}

	@Override
	public RemoteTerminalEmulator<?> createRte(SutConnection conn, StatisticMgr statMgr, long rteSleepTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean executeDatabaseCheckProcedure(SutConnection conn) throws SQLException {
		SutResultSet result = null;
		ReconbenchTransactionType txnType = ReconbenchTransactionType.CHECK_DATABASE;
		result = conn.callStoredProc(txnType.getProcedureId(), new Object[] {});
		return result.isCommitted();
	}

	@Override
	public String getBenchmarkName() {
		return "reconbench";
	}
}
