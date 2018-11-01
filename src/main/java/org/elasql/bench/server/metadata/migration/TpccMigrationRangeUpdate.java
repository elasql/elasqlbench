package org.elasql.bench.server.metadata.migration;

import java.io.Serializable;

import org.elasql.migration.MigrationRangeUpdate;

public class TpccMigrationRangeUpdate implements MigrationRangeUpdate, Serializable {
	
	private static final long serialVersionUID = 20181101001L;
	
	int minWid;
	TpccKeyIterator unmigratedKeys;
	
	TpccMigrationRangeUpdate(int minWid, TpccKeyIterator unmigratedKeys) {
		this.minWid = minWid;
		this.unmigratedKeys = unmigratedKeys;
	}

}
