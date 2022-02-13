/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.elasql.bench.benchmarks.recon.rte;

import org.elasql.bench.ElasqlBenchParameters;
import org.elasql.bench.benchmarks.recon.ReconbenchTransactionType;
import org.elasql.bench.remote.sp.ElasqlBenchSpResultSet;
import org.vanilladb.bench.TxnResultSet;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.TransactionExecutor;
import org.vanilladb.bench.rte.jdbc.JdbcExecutor;

public class ReconbenchTxExecutor extends TransactionExecutor<ReconbenchTransactionType> {
		
	public ReconbenchTxExecutor(ReconbenchParamGen pg) {
		this.pg = pg;
	}

	public TxnResultSet execute(SutConnection conn) {
		try {
			// generate parameters
			Object[] params = pg.generateParameter();
			System.out.println(pg.getTxnType());

			// send txn request and start measure txn response time
			long txnRT = System.nanoTime();
			
			ElasqlBenchSpResultSet result = (ElasqlBenchSpResultSet) executeTxn(conn, params);

			// measure txn response time
			long txnEndTime = System.nanoTime();
			txnRT = txnEndTime - txnRT;
			
			// display output
			if (ElasqlBenchParameters.SHOW_TXN_RESPONSE_ON_CONSOLE)
				System.out.println(pg.getTxnType() + " " + result.outputMsg());

			return new TxnResultSet(pg.getTxnType(), txnRT, txnEndTime,
					result.isCommitted(), result.outputMsg());
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	@Override
	protected JdbcExecutor<ReconbenchTransactionType> getJdbcExecutor() {
		throw new UnsupportedOperationException("no JDCB implementation for Recon");
	}
}
