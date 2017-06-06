package org.elasql.bench.server;

import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.server.SutStartUp;

public class StartUp {

	public static void main(String[] args) throws Exception {
		SutStartUp sut = null;
		
		switch (BenchmarkerParameters.CONNECTION_MODE) {
		case JDBC:
			throw new UnsupportedOperationException("ElaSQL does not support JDBC");
		case SP:
			sut = new ElasqlStartUp();
			break;
		}
		
		if (sut != null)
			sut.startup(args);
	}
}