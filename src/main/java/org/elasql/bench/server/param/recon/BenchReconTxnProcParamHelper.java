package org.elasql.bench.server.param.recon;

public class BenchReconTxnProcParamHelper extends BenchTxnProcParamHelper {

	@Override
	public void prepareParameters(Object... pars) {

		int indexCnt = 0;
//		System.out.println("Params: " + Arrays.toString(pars));

		readCount = (Integer) pars[indexCnt++];

		readRefId = new int[reconCount];
		readItemId = new int[readCount];

		for (int i = 0; i < reconCount; i++) {
			readRefId[i] = (Integer) pars[indexCnt++];
			readItemId[i] = -1;
		}

		for (int i = reconCount; i < readCount; i++)
			readItemId[i] = (Integer) pars[indexCnt++];

		writeCount = (Integer) pars[indexCnt++];
		writeItemId = new int[writeCount];

		indexCnt++;
		// first write item id is first read item id in micro benchmark
		if (writeCount > 0)
			writeItemId[0] = -1;
		for (int i = 1; i < writeCount; i++)
			writeItemId[i] = (Integer) pars[indexCnt++];
		newItemPrice = new double[writeCount];
		for (int i = 0; i < writeCount; i++)
			newItemPrice[i] = (Double) pars[indexCnt++];

		if (writeCount == 0)
			setReadOnly(true);
	}

}
