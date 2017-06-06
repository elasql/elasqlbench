package org.elasql.bench.util;

import org.vanilladb.core.util.PropertiesLoader;

public class ElasqlBenchProperties extends PropertiesLoader {
	
	private static ElasqlBenchProperties loader;

	public static ElasqlBenchProperties getLoader() {
		// Singleton
		if (loader == null)
			loader = new ElasqlBenchProperties();
		return loader;
	}

	protected ElasqlBenchProperties() {
		super();
	}

	@Override
	protected String getConfigFilePath() {
		return System.getProperty("org.elasql.bench.config.file");
	}

}
