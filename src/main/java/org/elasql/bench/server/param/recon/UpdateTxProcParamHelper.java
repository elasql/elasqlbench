package org.elasql.bench.server.param.recon;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class UpdateTxProcParamHelper extends StoredProcedureParamHelper {

	private int updateCount;
	private int[] refId;
	private int[] refIid;

	public int getUpdateCount() {
		return updateCount;
	}

	public int getRefId(int index) {
		return refId[index];
	}

	public void setRefIid(int s, int idx) {
		refIid[idx] = s;
	}

	public int getRefIid(int idx) {
		return refIid[idx];
	}

	public int getNewRefIid(int idx) {
		return refIid[updateCount - idx - 1];
	}

	@Override
	public void prepareParameters(Object... pars) {

		int indexCnt = 0;
//		System.out.println("Params: " + Arrays.toString(pars));

		updateCount = (Integer) pars[indexCnt++];
		refId = new int[updateCount];
		refIid = new int[updateCount];
		for (int i = 0; i < updateCount; i++) {
			refId[i] = (Integer) pars[indexCnt++];
		}

		setReadOnly(true);	

	}

	@Override
	public Schema getResultSetSchema() {
		return new Schema();
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		return new SpResultRecord();
	}

}
