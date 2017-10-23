package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.server.param.ycsb.YcsbBenchmarkProcParamHelper;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class YcsbBenchmarkProc extends CalvinStoredProcedure<YcsbBenchmarkProcParamHelper> {

	private Map<RecordKey, Constant> writeConstantMap = new HashMap<RecordKey, Constant>();
	private RecordKey[] insertKeys;
	
	public YcsbBenchmarkProc(long txNum) {
		super(txNum, new YcsbBenchmarkProcParamHelper());
	}

	@Override
	public void prepareKeys() {
		// set read keys
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			String id = paramHelper.getReadId(i);
			
			// create RecordKey for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ycsb_id", new VarcharConstant(id));
			RecordKey key = new RecordKey("ycsb", keyEntryMap);
			addReadKey(key);
		}

		// set write keys
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			// create record key for writing
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ycsb_id", new VarcharConstant(paramHelper.getWriteId(i)));
			RecordKey key = new RecordKey("ycsb", keyEntryMap);
			addWriteKey(key);
			
			// Create key-value pairs for writing
			Constant c = new VarcharConstant(paramHelper.getWriteValue(i));
			writeConstantMap.put(key, c);
		}
		
		// set insert keys
		insertKeys = new RecordKey[paramHelper.getInsertCount()];
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			// create record key for inserting
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ycsb_id", new VarcharConstant(paramHelper.getInsertId(i)));
			RecordKey key = new RecordKey("ycsb", keyEntryMap);
			insertKeys[i] = key;
			addInsertKey(key);
		}
	}
	
	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// SELECT ycsb_id, ycsb_1 FROM ycsb WHERE ycsb_id = ...
		int idx = 0;
		for (CachedRecord rec : readings.values()) {
			paramHelper.setYcsb((String) rec.getVal("ycsb_1").asJavaVal(), idx++);
		}

		// UPDATE ycsb SET ycsb_1 = ... WHERE ycsb_id = ...
		for (Map.Entry<RecordKey, Constant> pair : writeConstantMap.entrySet()) {
			CachedRecord rec = readings.get(pair.getKey());
			rec.setVal("ycsb_1", pair.getValue());
			update(pair.getKey(), rec);
		}
		
		// INSERT INTO ycsb (ycsb_id, ycsb_1, ...) VALUES ("...", "...", ...)
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			insert(insertKeys[i], paramHelper.getInsertVals(i));
		}

	}
}
