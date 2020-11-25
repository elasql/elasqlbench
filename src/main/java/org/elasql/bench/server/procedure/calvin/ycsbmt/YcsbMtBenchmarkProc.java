package org.elasql.bench.server.procedure.calvin.ycsbmt;

import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.param.ycsbmt.YcsbMtBenchmarkProcParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

public class YcsbMtBenchmarkProc extends CalvinStoredProcedure<YcsbMtBenchmarkProcParamHelper> {

	private Map<RecordKey, Constant> writeConstantMap = new HashMap<RecordKey, Constant>();

	public YcsbMtBenchmarkProc(long txNum) {
		super(txNum, new YcsbMtBenchmarkProcParamHelper());
	}

	@Override
	public void prepareKeys() {
		// set read keys
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			// create RecordKey for reading
			addReadKey(paramHelper.getReadId(i));
		}

		// set write keys
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			// create record key for writing
			RecordKey key = paramHelper.getWriteId(i);
			addWriteKey(key);

			// Create key-value pairs for writing
			Constant c = paramHelper.getWriteValue(i);
			writeConstantMap.put(key, c);
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// SELECT ycsb_n_id, ycsb_n_1 FROM ycsb_n WHERE ycsb_n_id = ...
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			// create RecordKey for reading
			RecordKey key = paramHelper.getReadId(i);
			String fldName = String.format("%s_1", key.getTableName());
			paramHelper.setReadValue(i, readings.get(key).getVal(fldName));
		}

		// UPDATE ycsb_n SET ycsb_n_1 = ... WHERE ycsb_n_id = ...
		for (Map.Entry<RecordKey, Constant> pair : writeConstantMap.entrySet()) {
			RecordKey key = pair.getKey();
			Constant val = pair.getValue();
			CachedRecord rec = readings.get(key);
			String fldName = String.format("%s_1", key.getTableName());
			rec.setVal(fldName, val);
			update(pair.getKey(), rec);
		}
	}
}
