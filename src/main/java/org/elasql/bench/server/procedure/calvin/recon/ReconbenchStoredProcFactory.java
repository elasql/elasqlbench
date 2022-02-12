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
package org.elasql.bench.server.procedure.calvin.recon;

import org.elasql.bench.benchmarks.recon.ReconbenchTransactionType;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class ReconbenchStoredProcFactory implements CalvinStoredProcedureFactory {
	
	public static final int EXECUTE = 3;

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (ReconbenchTransactionType.fromProcedureId(pid)) {
		case TESTBED_LOADER:
			sp = new ReconTestbedLoaderProc(txNum);
			break;
		case CHECK_DATABASE:
			sp = new ReconCheckDatabaseProc(txNum);
			break;
		case RECON:
			sp = new ReconTxnProc(txNum);
			break;
		case EXECUTE:
			sp = new ExecuteTxnProc(txNum);
			break;
		case UPDATE:
			sp = new UpdateTxnProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}
}
