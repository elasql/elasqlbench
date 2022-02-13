package org.elasql.bench.server.param.recon;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ReconSchemaBuilderProcParamHelper extends StoredProcedureParamHelper {

	private static final String TABLES_DDL[] = {
			"CREATE TABLE item ( i_id INT, i_im_id INT, i_name VARCHAR(24), "
					+ "i_price DOUBLE, i_data VARCHAR(50) )",
			"CREATE TABLE ref ( r_id INT, r_iid INT)" };
	
	private static final String INDEXES_DDL[] = {
			"CREATE INDEX idx_item ON item (i_id)",
			"CREATE INDEX idx_ref ON ref (r_id)"};

	public String[] getTableSchemas() {
		return TABLES_DDL;
	}

	public String[] getIndexSchemas() {
		return INDEXES_DDL;
	}

	@Override
	public void prepareParameters(Object... pars) {
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
