package org.elasql.bench.server.procedure.calvin.tpce;

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.server.param.tpce.TradeOrderParamHelper;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.VarcharConstant;

// XXX: Simplified version
public class TradeOrderProc extends CalvinStoredProcedure<TradeOrderParamHelper> {

	String acctName, custFName, custLName, taxId, brokerName, exchId, sName, statusId;
	long brokerId, custId, coId;
	int taxStatus, custTier, typeIsMarket, typeIsSell;
	double marketPrice;
	
	private RecordKey accountKey, customerKey, brokerKey, securityKey,
			lastTradeKey, tradeTypeKey, tradeKey, tradeHistoryKey;
	
	public TradeOrderProc(long txNum) {
		super(txNum, new TradeOrderParamHelper());
	}

	@Override
	protected void prepareKeys() {
		/***************** Construct Read Keys *******************/
		// account
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ca_id", new BigIntConstant(paramHelper.getAcctId()));
		accountKey = new RecordKey("customer_account", keyEntryMap);
		addReadKey(accountKey);

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

		// security
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("s_symb", new VarcharConstant(paramHelper.getSymbol()));
		securityKey = new RecordKey("security", keyEntryMap);
		addReadKey(securityKey);

		// last trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("lt_s_symb",
				new VarcharConstant(paramHelper.getSymbol()));
		lastTradeKey = new RecordKey("last_trade", keyEntryMap);
		addReadKey(lastTradeKey);

		// trade type
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("tt_id",
				new VarcharConstant(paramHelper.getTradeTypeId()));
		tradeTypeKey = new RecordKey("trade_type", keyEntryMap);
		addReadKey(tradeTypeKey);

		
		/***************** Construct Write Keys *******************/
		// insert new trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeKey = new RecordKey("trade", keyEntryMap);
		addWriteKey(tradeKey);

		// insert new history
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap
				.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeHistoryKey = new RecordKey("trade_history", keyEntryMap);
		addWriteKey(tradeHistoryKey);
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		String accountName = (String) readings.get(accountKey)
				.getVal("ca_name").asJavaVal();

		String customerName = (String) readings.get(customerKey)
				.getVal("c_name").asJavaVal();

		String brokerName = (String) readings.get(brokerKey).getVal("b_name")
				.asJavaVal();

		CachedRecord rec = readings.get(securityKey);
		long companyId = (Long) rec.getVal("s_co_id").asJavaVal();
		String securityName = (String) rec.getVal("s_name").asJavaVal();

		Constant marketPriceCon = readings.get(lastTradeKey).getVal("lt_price");

		rec = readings.get(tradeTypeKey);
		int typeIsMarket = (Integer) rec.getVal("tt_is_mrkt").asJavaVal();
		int typeIsSell = (Integer) rec.getVal("tt_is_sell").asJavaVal();

		// INSERT INTO trade(t_id, t_dts, t_tt_id, t_s_symb, t_qty,
		// t_bid_price, t_ca_id, t_trade_price) VALUES (...)
		Map<String, Constant> fldVals = new HashMap<String, Constant>();

		long currentTime = System.currentTimeMillis();
		fldVals.put("t_id", new BigIntConstant(paramHelper.getTradeId()));
		fldVals.put("t_dts", new BigIntConstant(currentTime));
		fldVals.put("t_tt_id",
				new VarcharConstant(paramHelper.getTradeTypeId()));
		fldVals.put("t_s_symb", new VarcharConstant(paramHelper.getSymbol()));
		fldVals.put("t_qty", new BigIntConstant(paramHelper.getTradeQty()));
		fldVals.put("t_bid_price", marketPriceCon);
		fldVals.put("t_ca_id", new BigIntConstant(paramHelper.getAcctId()));
		fldVals.put("t_trade_price",
				new DoubleConstant(paramHelper.getTradePrice()));
		insert(tradeKey, fldVals);

		// INSERT INTO trade_history(th_t_id, th_dts)
		fldVals = new HashMap<String, Constant>();
		fldVals.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		fldVals.put("th_dts", new BigIntConstant(currentTime));
		insert(tradeHistoryKey, fldVals);
	}

}
