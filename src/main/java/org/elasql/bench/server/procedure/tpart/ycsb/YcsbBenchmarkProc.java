package org.elasql.bench.server.procedure.tpart.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.server.param.ycsb.YcsbBenchmarkProcParamHelper;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class YcsbBenchmarkProc extends TPartStoredProcedure<YcsbBenchmarkProcParamHelper> {
	private static Logger logger = Logger.getLogger(YcsbBenchmarkProc.class.getName());
	
	private static final VarcharConstant WIRTE_VALUE = new VarcharConstant(String.format("%033d", 0));
	
	private static PrimaryKey toRecordKey(int ycsbId) {
		String idString = String.format(YcsbConstants.ID_FORMAT, ycsbId);
		return new PrimaryKey("ycsb", "ycsb_id", new VarcharConstant(idString));
	}

	private PrimaryKey[] readKeys;
	private PrimaryKey[] insertKeys;
	private Map<PrimaryKey, Constant> writeConstantMap = new HashMap<PrimaryKey, Constant>();
	
	public YcsbBenchmarkProc(long txNum) {
		super(txNum, new YcsbBenchmarkProcParamHelper());
	}
	
	@Override
	protected void prepareKeys() {
		// set read keys
		readKeys = new PrimaryKey[paramHelper.getReadCount()];
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			// create RecordKey for reading
			PrimaryKey key = toRecordKey(paramHelper.getReadId(i));
			readKeys[i] = key;
			addReadKey(key);
		}
		
		// set write keys
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			// create record key for writing
			PrimaryKey key = toRecordKey(paramHelper.getWriteId(i));
			addWriteKey(key);
			
			// Create key-value pairs for writing
//			Constant c = new VarcharConstant(paramHelper.getWriteValue(i));
			Constant c = WIRTE_VALUE;
			writeConstantMap.put(key, c);
		}
		
		// set insert keys
		insertKeys = new PrimaryKey[paramHelper.getInsertCount()];
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			// create record key for inserting
			PrimaryKey key = toRecordKey(paramHelper.getInsertId(i));
			insertKeys[i] = key;
			addInsertKey(key);
		}
	}
	
	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		CachedRecord rec;
		// SELECT ycsb_id, ycsb_1 FROM ycsb WHERE ycsb_id = ...
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			rec = readings.get(readKeys[idx]);
			paramHelper.setYcsb((String) rec.getVal("ycsb_1").asJavaVal(), idx);
		}

		// UPDATE ycsb SET ycsb_1 = ... WHERE ycsb_id = ...
		for (Map.Entry<PrimaryKey, Constant> pair : writeConstantMap.entrySet()) {
			rec = readings.get(pair.getKey());
			rec.setVal("ycsb_1", pair.getValue());
			update(pair.getKey(), rec);
		}
		
		// INSERT INTO ycsb (ycsb_id, ycsb_1, ...) VALUES ("...", "...", ...)
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			insert(insertKeys[i], paramHelper.getInsertVals(i));
		}
	}
	
	@Override
	public double getWeight() {
		return paramHelper.getWriteCount() + paramHelper.getReadCount();
	}

}
