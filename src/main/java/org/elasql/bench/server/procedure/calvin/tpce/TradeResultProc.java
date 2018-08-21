/*******************************************************************************
 * Copyright 2016, 2017 elasql.org contributors
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
package org.elasql.bench.server.procedure.calvin.tpce;

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.server.param.tpce.TradeResultParamHelper;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.VarcharConstant;

public class TradeResultProc extends CalvinStoredProcedure<TradeResultParamHelper> {
	
	private RecordKey cusAcctKey, customerKey, brokerKey, tradeKey, tradeHistoryKey;
	
	public TradeResultProc(long txNum) {
		super(txNum, new TradeResultParamHelper());
	}

	@Override
	protected void prepareKeys() {
		// account
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ca_id", new BigIntConstant(paramHelper.getAcctId()));
		cusAcctKey = new RecordKey("customer_account", keyEntryMap);
		addReadKey(cusAcctKey);
		addWriteKey(cusAcctKey);

		// customer
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap
				.put("c_id", new BigIntConstant(paramHelper.getCustomerId()));
		customerKey = new RecordKey("customer", keyEntryMap);
		addReadKey(customerKey);

		// broker
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("b_id", new BigIntConstant(paramHelper.getBrokerId()));
		brokerKey = new RecordKey("broker", keyEntryMap);
		addReadKey(brokerKey);

		// trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeKey = new RecordKey("trade", keyEntryMap);
		addReadKey(tradeKey);

		// insert new history
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap
				.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeHistoryKey = new RecordKey("trade_history", keyEntryMap);
		addInsertKey(tradeHistoryKey);
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// SELECT ca_name, ca_b_id, ca_c_id FROM customer_account WHERE
		// ca_id = acctId
		CachedRecord rec = readings.get(cusAcctKey);
		rec.getVal("ca_name").asJavaVal();
		rec.getVal("ca_b_id").asJavaVal();
		rec.getVal("ca_c_id").asJavaVal();

		// SELECT c_name FROM customer WHERE c_id = customerKey
		rec = readings.get(customerKey);
		rec.getVal("c_f_name").asJavaVal();

		// SELECT b_name FROM broker WHERE b_id = brokerId
		rec = readings.get(brokerKey);
		rec.getVal("b_name").asJavaVal();

		// SELECT trade_infos FROM trade WHERE t_id = tradeId
		rec = readings.get(tradeKey);
		rec.getVal("t_trade_price").asJavaVal();

		// INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (...)
		long currentTime = System.currentTimeMillis();
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals = new HashMap<String, Constant>();
		fldVals.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		fldVals.put("th_dts", new BigIntConstant(currentTime));
		fldVals.put("th_st_id", new VarcharConstant("A"));
		insert(tradeHistoryKey, fldVals);

		// UPDATE customer_account SET ca_bal = ca_bal + tradePrice WHERE
		// ca_id = acctId
		rec = readings.get(cusAcctKey);
		rec.setVal("ca_bal", new DoubleConstant(1000.0));
		update(cusAcctKey, rec);
	}

}
