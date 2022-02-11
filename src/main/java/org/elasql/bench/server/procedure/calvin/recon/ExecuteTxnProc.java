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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.server.param.recon.BenchExecuteTxnProcParamHelper;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;

public class ExecuteTxnProc extends CalvinStoredProcedure<BenchExecuteTxnProcParamHelper> {

	private Map<PrimaryKey, Constant> writeConstantMap = new HashMap<PrimaryKey, Constant>();
	private List<PrimaryKey> refKeys = new ArrayList<PrimaryKey>();
	private List<PrimaryKey> itemKeys = new ArrayList<PrimaryKey>();
	private boolean finished; // false if need to do reconnaissance again.
	
	public ExecuteTxnProc(long txNum) {
		super(txNum, new BenchExecuteTxnProcParamHelper());
	}
	
	public ExecuteTxnProc(long txNum, BenchExecuteTxnProcParamHelper pars) {
		super(txNum, pars);
	}

	@Override
	public void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// set ref read keys
		for (int idx = 0; idx < paramHelper.getReconCount(); idx++) {
			int rid = paramHelper.getReadRefId(idx);
			
			// create record key for reading
			PrimaryKey key = new PrimaryKey("ref", "r_id", new IntegerConstant(rid));
			analyzer.addReadKey(key);
			refKeys.add(key);
		}
		
		// set read keys
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			
			// create record key for reading
			PrimaryKey key = new PrimaryKey("item", "i_id", new IntegerConstant(iid));
			analyzer.addReadKey(key);
			itemKeys.add(key);
		}

		// set write keys
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			
			// create record key for writing
			PrimaryKey key = new PrimaryKey("item", "i_id", new IntegerConstant(iid));
			analyzer.addUpdateKey(key);

			// Create key-value pairs for writing
			Constant c = new DoubleConstant(newPrice);
			writeConstantMap.put(key, c);
		}
	}
	@Override
	public boolean willResponseToClients() {
		return finished;
	}
	
	@Override
	public void afterCommit() {
		if (!finished) {
			Object[] params = new Object[] {getClientId(), getConnectionId(), txNum,  paramHelper.generateParameter()};
			Elasql.connectionMgr().sendStoredProcedureCall(false, getClientId(), getConnectionId(),
					ReconbenchStoredProcFactory.EXECUTE, txNum, params);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// SELECT r_iid FROM ref WHERE r_id = ...
		// Is the record changed ?
		int idx = 0;
		for(PrimaryKey refKey : refKeys) {
			int riid = (int) readings.get(refKey).getVal("r_iid").asJavaVal();
			if (riid != paramHelper.getReadItemId(idx)) {
				paramHelper.setReadItemId(riid, idx);
				// abort : cause this tx has not write anything, so nothing should be undo.
				// send new tx
				finished = false;
				return;
			}
		}
		
		// SELECT i_name, i_price FROM items WHERE i_id = ...
		idx = 0;
		for(PrimaryKey itemKey : itemKeys) {
			paramHelper.setItemName((String) readings.get(itemKey).getVal("i_name").asJavaVal(), idx);
			paramHelper.setItemPrice((double) readings.get(itemKey).getVal("i_price").asJavaVal(), idx++);
		}

		// UPDATE items SET i_price = ... WHERE i_id = ...
		for (Map.Entry<PrimaryKey, Constant> pair : writeConstantMap.entrySet()) {
			CachedRecord rec = readings.get(pair.getKey());
			rec.setVal("i_price", pair.getValue());
			update(pair.getKey(), rec);
		}
		finished = true;
	}
}
