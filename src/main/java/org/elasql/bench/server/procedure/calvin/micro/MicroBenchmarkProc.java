package org.elasql.bench.server.procedure.calvin.micro;

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.server.param.micro.MicroBenchmarkProcParamHelper;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;

public class MicroBenchmarkProc extends CalvinStoredProcedure<MicroBenchmarkProcParamHelper> {

	private Map<RecordKey, Constant> writeConstantMap = new HashMap<RecordKey, Constant>();

	public MicroBenchmarkProc(long txNum) {
		super(txNum, new MicroBenchmarkProcParamHelper());
	}

	@Override
	public void prepareKeys() {
		// set read keys
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			
			// create record key for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(iid));
			RecordKey key = new RecordKey("item", keyEntryMap);
			addReadKey(key);
		}

		// set write keys
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			
			// create record key for writing
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(iid));
			RecordKey key = new RecordKey("item", keyEntryMap);
			addWriteKey(key);

			// Create key-value pairs for writing
			Constant c = new DoubleConstant(newPrice);
			writeConstantMap.put(key, c);
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// SELECT i_name, i_price FROM items WHERE i_id = ...
		int idx = 0;
		for (CachedRecord rec : readings.values()) {
			paramHelper.setItemName((String) rec.getVal("i_name").asJavaVal(), idx);
			paramHelper.setItemPrice((double) rec.getVal("i_price").asJavaVal(), idx++);
		}

		// UPDATE items SET i_price = ... WHERE i_id = ...
		for (Map.Entry<RecordKey, Constant> pair : writeConstantMap.entrySet()) {
			CachedRecord rec = readings.get(pair.getKey());
			rec.setVal("i_price", pair.getValue());
			update(pair.getKey(), rec);
		}

	}
}
