package org.elasql.bench.server.param.ycsbmt;

import org.elasql.bench.ycsbmt.YcsbMtConstants;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class YcsbMtBenchmarkProcParamHelper extends StoredProcedureParamHelper {

	// Optimization: we provide a constant value to reduce the transmission cost
	private static final VarcharConstant WIRTE_VALUE = new VarcharConstant(String.format("%033d", 0));
	
	// Record Count
	private int readCount;
	private int writeCount;
	
	// ID
	private Integer[] readTblIds;
	private Integer[] readRecIds;
	private Integer[] writeTblIds;
	private Integer[] writeRecIds;

	// Value
	private Constant[] readVals;
	// Optimization: we provide a constant value to reduce the transmission cost
//	private String[] writeVals;

	public int getReadCount() {
		return readCount;
	}

	public int getWriteCount() {
		return writeCount;
	}
	
	public RecordKey getReadId(int index) {
		String tblName = String.format("ycsb_%d", readTblIds[index]);
		String fldName = String.format("ycsb_%d_id", readTblIds[index]);
		String idString = String.format(YcsbMtConstants.ID_FORMAT, readRecIds[index]);
		return new RecordKey(tblName, fldName, new VarcharConstant(idString));
	}
	
	public RecordKey getWriteId(int index) {
		String tblName = String.format("ycsb_%d", writeTblIds[index]);
		String fldName = String.format("ycsb_%d_id", writeTblIds[index]);
		String idString = String.format(YcsbMtConstants.ID_FORMAT, writeRecIds[index]);
		return new RecordKey(tblName, fldName, new VarcharConstant(idString));
	}
	
	public void setReadValue(int index, Constant val) {
		readVals[index] = val;
	}
	
	public VarcharConstant getWriteValue(int index) {
		// Optimization: we provide a constant value to reduce the transmission cost
//		return writeVals[index];
		return WIRTE_VALUE;
	}

	@Override
	public void prepareParameters(Object... pars) {
		int indexCnt = 0;
		
		// Format: (without write values) 
		// [Read Count (rn),
		//  Read Record 1 Table Id, Read Record 1 Record Id,
		//  Read Record 2 Table Id, Read Record 2 Record Id,
		//  ...
		//  Read Record rn Table Id, Read Record rn Record Id,
		//  Write Count (wn),
		//  Write Record 1 Table Id, Write Record 1 Record Id,
		//  Write Record 2 Table Id, Write Record 2 Record Id,
		//  ...
		//  Write Record wn Table Id, Write Record wn Record Id]
		
		// Read Count
		readCount = (Integer) pars[indexCnt++];
		
		// Read Records
		readTblIds = new Integer[readCount];
		readRecIds = new Integer[readCount];
		readVals = new Constant[readCount];
		for (int i = 0; i < readCount; i++) {
			readTblIds[i] = (Integer) pars[indexCnt++];
			readRecIds[i] = (Integer) pars[indexCnt++];
		}
		
		// Write Count
		writeCount = (Integer) pars[indexCnt++];
		
		// Write Records
		writeTblIds = new Integer[readCount];
		writeRecIds = new Integer[readCount];
		for (int i = 0; i < writeCount; i++) {
			writeTblIds[i] = (Integer) pars[indexCnt++];
			writeRecIds[i] = (Integer) pars[indexCnt++];
		}

		// Set read-only
		if (writeCount == 0)
			setReadOnly(true);
	}

	@Override
	public SpResultSet createResultSet() {
		// Optimization: we only provide the result of commitment to reduce the transmission cost
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));

		return new SpResultSet(sch, rec);
	}
}
