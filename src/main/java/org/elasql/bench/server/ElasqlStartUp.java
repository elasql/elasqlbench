package org.elasql.bench.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.server.metadata.MicroBenchMetisPartitionPlan;
import org.elasql.bench.server.metadata.MicroBenchPartitionPlan;
import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.bench.server.metadata.TpcePartitionPlan;
import org.elasql.bench.server.metadata.YcsbMetisPartitionPlan;
import org.elasql.bench.server.migraion.YcsbMigrationMgr;
import org.elasql.bench.server.procedure.calvin.tpce.TpceStoredProcFactory;
import org.elasql.bench.server.procedure.calvin.ycsb.YcsbStoredProcFactory;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.migration.MigrationMgr;
import org.elasql.procedure.DdStoredProcedureFactory;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.HashPartitionPlan;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.elasql.storage.metadata.RangePartitionPlan;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.server.SutStartUp;

public class ElasqlStartUp implements SutStartUp {
	private static Logger logger = Logger.getLogger(ElasqlStartUp.class
			.getName());
	
	// Metis
	public static final boolean LOAD_METIS_PARTITIONS = false;
	private static final String METIS_FILE_PATH = "/opt/shared/metis-partitions/google-20/default/tail.part"; 
	
	private static String dbName;
	private static int nodeId;
	private static boolean isSequencer;

	public void startup(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("initializing benchmarker server...");
		
		try {
			parseArguments(args);
		} catch (IllegalArgumentException e) {
			System.out.println("Error: " + e.getMessage());
			System.out.println("Usage: ./startup [DB Name] [Node Id] ([Is Sequencer])");
		}
		
		Elasql.init(dbName, nodeId, isSequencer, getStoredProcedureFactory(), getPartitionPlan(),
				getMigrationMgr());

		if (logger.isLoggable(Level.INFO))
			logger.info("ElaSQL server ready");
	}
	
	private static void parseArguments(String[] args) throws IllegalArgumentException {
		if (args.length < 2) {
			throw new IllegalArgumentException("The number of arguments is less than 2");
		}
		
		// #1 DB Name
		dbName = args[0];
		
		// #2 Node Id
		try {
			nodeId = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(String.format("'%s' is not a number", args[1]));
		}
		
		// #3 Is sequencer ?
		isSequencer = false;
		if (args.length > 2) {
			try {
				int num = Integer.parseInt(args[2]);
				if (num == 1)
					isSequencer = true;
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(String.format("'%s' is not a number", args[2]));
			}
		}
	}
	
	private DdStoredProcedureFactory getStoredProcedureFactory() {
		DdStoredProcedureFactory factory = null;
		switch (Elasql.SERVICE_TYPE) {
		case NAIVE:
			factory = getNaiveSpFactory();
			break;
		case CALVIN:
			factory = getCalvinSpFactory();
			break;
		case TPART:
		case TPART_LAP:
			factory = getTPartSpFactory();
			break;
		}
		return factory;
	}
	
	private DdStoredProcedureFactory getNaiveSpFactory() {
		DdStoredProcedureFactory factory = null;
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			throw new UnsupportedOperationException("No Micro for now");
		case TPCC:
			throw new UnsupportedOperationException("No TPC-C for now");
		case TPCE:
			throw new UnsupportedOperationException("No TPC-E for now");
		case YCSB:
			throw new UnsupportedOperationException("No YCSB for now");
		}
		return factory;
	}
	
	private DdStoredProcedureFactory getCalvinSpFactory() {
		DdStoredProcedureFactory factory = null;
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			if (logger.isLoggable(Level.INFO))
				logger.info("using Micro-benchmark stored procedures for Calvin");
			factory = new org.elasql.bench.server.procedure.calvin.micro.MicrobenchStoredProcFactory();
			break;
		case TPCC:
			if (logger.isLoggable(Level.INFO))
				logger.info("using TPC-C stored procedures for Calvin");
			factory = new org.elasql.bench.server.procedure.calvin.tpcc.TpccStoredProcFactory();
			break;
		case TPCE:
			if (logger.isLoggable(Level.INFO))
				logger.info("using TPC-E stored procedures for Calvin");
			factory = new TpceStoredProcFactory();
			break;
		case YCSB:
			if (logger.isLoggable(Level.INFO))
				logger.info("using YCSB stored procedures for Calvin");
			factory = new YcsbStoredProcFactory();
			break;
		}
		return factory;
	}
	
	private DdStoredProcedureFactory getTPartSpFactory() {
		DdStoredProcedureFactory factory = null;
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			if (logger.isLoggable(Level.INFO))
				logger.info("using Micro-benchmark stored procedures for T-Part");
			factory = new org.elasql.bench.server.procedure.tpart.micro.MicrobenchStoredProcFactory();
			break;
		case TPCC:
			if (logger.isLoggable(Level.INFO))
				logger.info("using TPC-C stored procedures for T-Part");
			factory = new org.elasql.bench.server.procedure.calvin.tpcc.TpccStoredProcFactory();
			break;
		case TPCE:
			throw new UnsupportedOperationException("No TPC-E for now");
		case YCSB:
			if (logger.isLoggable(Level.INFO))
				logger.info("using YCSB stored procedures for T-Part");
			factory = new org.elasql.bench.server.procedure.tpart.ycsb.YcsbStoredProcFactory();
		}
		return factory;
	}
	
	private PartitionPlan getPartitionPlan() {
		PartitionPlan partPlan = null;
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			partPlan = new MicroBenchPartitionPlan();
			if (LOAD_METIS_PARTITIONS)
				partPlan = new MicroBenchMetisPartitionPlan(partPlan, METIS_FILE_PATH);
			break;
		case TPCC:
			partPlan = new TpccPartitionPlan();
			break;
		case TPCE:
			partPlan = new TpcePartitionPlan();
			break;
		case YCSB:
			if (MigrationMgr.ENABLE_NODE_SCALING) {
				// For elastic experiments
				int numOfPartitions = MigrationMgr.IS_SCALING_OUT?
						PartitionMetaMgr.NUM_PARTITIONS - 1: PartitionMetaMgr.NUM_PARTITIONS;
				partPlan = new RangePartitionPlan("ycsb_id", ElasqlYcsbConstants.RECORD_PER_PART *
						numOfPartitions, numOfPartitions);
			} else {
				partPlan =  new RangePartitionPlan("ycsb_id", ElasqlYcsbConstants.RECORD_PER_PART *
						PartitionMetaMgr.NUM_PARTITIONS, PartitionMetaMgr.NUM_PARTITIONS);
//				partPlan =  new HashPartitionPlan("ycsb_id");
			}
			
			if (LOAD_METIS_PARTITIONS)
				partPlan = new YcsbMetisPartitionPlan(partPlan, METIS_FILE_PATH);
			
			break;
		}
		return partPlan;
	}
	
	private MigrationMgr getMigrationMgr() {
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			throw new UnsupportedOperationException("No Micro Migration Manager for now");
		case TPCC:
			throw new UnsupportedOperationException("No TPC-C Migration Manager for now");
		case TPCE:
			throw new UnsupportedOperationException("No TPC-E Migration Manager for now");
		case YCSB:
			return new YcsbMigrationMgr();
		}
		return null;
	}
}
