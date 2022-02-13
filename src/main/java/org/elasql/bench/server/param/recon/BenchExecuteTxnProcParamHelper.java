package org.elasql.bench.server.param.recon;

public class BenchExecuteTxnProcParamHelper extends BenchTxnProcParamHelper {

	@Override
	public void prepareParameters(Object... pars) {

		int indexCnt = 0;
//		System.out.println("Params: " + Arrays.toString(pars));

		reconCount = (Integer) pars[indexCnt++];
		readRefId = new int[reconCount];
		for (int i = 0; i < reconCount; i++) {
			readRefId[i] = (Integer) pars[indexCnt++];
		}	

		readCount = (Integer) pars[indexCnt++];
		readItemId = new int[readCount];
		for (int i = 0; i < readCount; i++)
			readItemId[i] = (Integer) pars[indexCnt++];

		itemName = new String[readCount];
		itemPrice = new double[readCount];

		writeCount = (Integer) pars[indexCnt++];
		writeItemId = new int[writeCount];
		newItemPrice = new double[writeCount];
		for (int i = 0; i < writeCount; i++) {
			writeItemId[i] = (Integer) pars[indexCnt++];
			newItemPrice[i] = (Double) pars[indexCnt++];
		}
	}

}
