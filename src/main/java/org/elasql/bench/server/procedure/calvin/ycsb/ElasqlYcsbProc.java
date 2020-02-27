package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.param.ycsb.ElasqlYcsbProcParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class ElasqlYcsbProc extends CalvinStoredProcedure<ElasqlYcsbProcParamHelper> {
	
	private static RecordKey toRecordKey(String tableName, Integer ycsbId) {
		String fieldName = String.format("%s_id", tableName);
		String idString = String.format(YcsbConstants.ID_FORMAT, ycsbId);
		return new RecordKey(tableName, fieldName, new VarcharConstant(idString));
	}
	
	private RecordKey[] readKeys;
	private RecordKey[] writeKeys;
	private RecordKey[] insertKeys;
	private Map<RecordKey, Constant> writeConstantMap = new HashMap<RecordKey, Constant>();

	public ElasqlYcsbProc(long txNum) {
		super(txNum, new ElasqlYcsbProcParamHelper());
	}

	@Override
	public void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// set read keys
		readKeys = new RecordKey[paramHelper.getReadCount()];
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			// create RecordKey for reading
			RecordKey key = toRecordKey(paramHelper.getReadTableName(i), paramHelper.getReadId(i));
			readKeys[i] = key;
			analyzer.addReadKey(key);
		}

		// set write keys
		writeKeys = new RecordKey[paramHelper.getWriteCount()];
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			// create record key for writing
			RecordKey key = toRecordKey(paramHelper.getWriteTableName(i), paramHelper.getWriteId(i));
			writeKeys[i] = key;
			analyzer.addUpdateKey(key);

			// Create key-value pairs for writing
			Constant c = new VarcharConstant(paramHelper.getWriteValue(i));
			writeConstantMap.put(key, c);
		}

		// set insert keys
		insertKeys = new RecordKey[paramHelper.getInsertCount()];
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			// create record key for inserting
			RecordKey key = toRecordKey(paramHelper.getInsertTableName(i), paramHelper.getInsertId(i));
			insertKeys[i] = key;
			analyzer.addInsertKey(key);
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// SELECT ycsb_id, ycsb_1 FROM ycsb WHERE ycsb_id = ...
		for (int i = 0; i < readKeys.length; i++) {
			String fieldName = paramHelper.getReadTableName(i) + "_1";
			CachedRecord rec = readings.get(readKeys[i]);
			paramHelper.setReadVal((String) rec.getVal(fieldName).asJavaVal(), i);
		}

		// UPDATE ycsb SET ycsb_1 = ... WHERE ycsb_id = ...
		for (int i = 0; i < writeKeys.length; i++) {
			String fieldName = paramHelper.getWriteTableName(i) + "_1";
			CachedRecord rec = readings.get(writeKeys[i]);
			rec.setVal(fieldName, writeConstantMap.get(writeKeys[i]));
		}

		// INSERT INTO ycsb (ycsb_id, ycsb_1, ...) VALUES ("...", "...", ...)
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			insert(insertKeys[i], paramHelper.getInsertVals(i));
		}
	}
}
