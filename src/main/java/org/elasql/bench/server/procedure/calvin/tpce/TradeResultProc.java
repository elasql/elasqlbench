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
	
	private RecordKey accountKey, customerKey, brokerKey, tradeKey, tradeHistoryKey;
	
	public TradeResultProc(long txNum) {
		super(txNum, new TradeResultParamHelper());
	}

	@Override
	protected void prepareKeys() {
		// account
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ca_id", new BigIntConstant(paramHelper.getAcctId()));
		accountKey = new RecordKey("customer_account", keyEntryMap);
		addReadKey(accountKey);
		addWriteKey(accountKey);

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
		addWriteKey(tradeHistoryKey);
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// SELECT ca_name, ca_b_id, ca_c_id FROM customer_account WHERE
		// ca_id = acctId
		String accountName = (String) readings.get(accountKey)
				.getVal("ca_name").asJavaVal();

		// SELECT c_name FROM customer WHERE c_id = customerKey
		String customerName = (String) readings.get(customerKey)
				.getVal("c_name").asJavaVal();

		// SELECT b_name FROM broker WHERE b_id = brokerId
		String brokerName = (String) readings.get(brokerKey)
				.getVal("b_name").asJavaVal();

		// SELECT trade_infos FROM trade WHERE t_id = tradeId
		CachedRecord rec = readings.get(tradeKey);
		long tradeTime = (Long) rec.getVal("t_dts").asJavaVal();
		String symbol = (String) rec.getVal("t_s_symb").asJavaVal();

		// INSERT INTO trade_history(th_t_id, th_dts)
		long currentTime = System.currentTimeMillis();
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals = new HashMap<String, Constant>();
		fldVals.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		fldVals.put("th_dts", new BigIntConstant(currentTime));
		insert(tradeHistoryKey, fldVals);

		// UPDATE customer_account SET ca_bal = ca_bal + tradePrice WHERE
		// ca_id = acctId
		rec = readings.get(accountKey);
		rec.setVal("ca_bal", new DoubleConstant(1000));
		rec.setVal("ca_name", new VarcharConstant(accountName));
		update(accountKey, rec);
	}

}
