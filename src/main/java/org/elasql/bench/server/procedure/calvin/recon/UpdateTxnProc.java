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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasql.bench.server.param.recon.UpdateTxProcParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.IntegerConstant;

public class UpdateTxnProc extends CalvinStoredProcedure<UpdateTxProcParamHelper> {
	
	private List<PrimaryKey> writeKeyList = new ArrayList<PrimaryKey>();
	
	public UpdateTxnProc(long txNum) {
		super(txNum, new UpdateTxProcParamHelper());
	}
	
	public UpdateTxnProc(long txNum, UpdateTxProcParamHelper pars) {
		super(txNum, pars);
	}

	@Override
	public void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// set read / write keys
		for (int idx = 0; idx < paramHelper.getUpdateCount(); idx++) {
			int rid = paramHelper.getRefId(idx);
			
			// create record key for reading
			PrimaryKey key = new PrimaryKey("ref", "r_id", new IntegerConstant(rid));
			analyzer.addReadKey(key);
			analyzer.addUpdateKey(key);
			writeKeyList.add(key);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// SELECT r_iid FROM ref WHERE r_id = ...
		int idx = 0;
		for (CachedRecord rec : readings.values()) {
			paramHelper.setRefIid((int) rec.getVal("r_iid").asJavaVal(), idx++);
		}
		
		// UPDATE ref SET r_iid = ... WHERE r_id = ...
		idx = 0;
		for (PrimaryKey key : writeKeyList) {
			CachedRecord rec = readings.get(key);
			rec.setVal("r_iid", new IntegerConstant(paramHelper.getNewRefIid(idx++)));
			update(key, rec);
		}
	}
}
