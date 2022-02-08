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

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.server.param.recon.BenchReconTxnProcParamHelper;
import org.vanilladb.core.sql.IntegerConstant;

public class ReconTxnProc extends CalvinStoredProcedure<BenchReconTxnProcParamHelper> {
	
	public ReconTxnProc(long txNum) {
		super(txNum, new BenchReconTxnProcParamHelper());
	}

	@Override
	public void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// set ref read keys
		for (int idx = 0; idx < paramHelper.getReconCount(); idx++) {
			int rid = paramHelper.getReadRefId(idx);
			
			// create record key for reading
			PrimaryKey key = new PrimaryKey("ref", "r_id", new IntegerConstant(rid));
			analyzer.addReadKey(key);
		}
	}
	
	@Override
	public boolean willResponseToClients() {
		return false;
	}
	
	@Override
	public void afterCommit() {
		Object[] params = new Object[] {getClientId(), getConnectionId(), txNum,  paramHelper.generateParameter()};
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				ReconbenchStoredProcFactory.EXECUTE, params);
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// SELECT i_id FROM ref WHERE r_id = ...
		int idx = 0;
		for (CachedRecord rec : readings.values()) {
			if (paramHelper.getWriteCount() > 0 && idx == 0)
				paramHelper.setWriteItemId((int) rec.getVal("i_id").asJavaVal(), idx);
			paramHelper.setReadItemId((int) rec.getVal("i_id").asJavaVal(), idx++);
		}
	}
}
