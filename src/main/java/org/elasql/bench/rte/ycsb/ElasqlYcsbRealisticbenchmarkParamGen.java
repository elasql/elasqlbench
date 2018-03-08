package org.elasql.bench.rte.ycsb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.Benchmarker;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.tpcc.TpccValueGenerator;
import org.vanilladb.bench.util.YcsbLatestGenerator;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.bench.ycsb.YcsbTransactionType;

public class ElasqlYcsbRealisticbenchmarkParamGen implements TxParamGenerator {
	private static final double RW_TX_RATE;
	private static final double DIST_TX_RATE;
	private static final double SKEW_PARAMETER;
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	private static final AtomicInteger[] GLOBAL_COUNTERS;
	
	// Real parameter
	private static double[] SKEW_HIS;
	private static double[] NOR1;
	private static double[] NOR2;
	private static double[] NOR3;
	
	private static final long REPLAY_PREIOD;
	private static final long WARMUP_TIME;
	private static final double SKEW_WEIGHT;
	
	private static int nodeId;
	private YcsbLatestGenerator[] latestRandoms = new YcsbLatestGenerator[NUM_PARTITIONS];
	private YcsbLatestGenerator latestRandom;
	
	static {
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".RW_TX_RATE", 0.0);
		SKEW_PARAMETER = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlYcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.0);
		
		DIST_TX_RATE = 0.1;
		
		WARMUP_TIME = 200 * 1000;	// cause by ycsb's long init time (init 200 RTEs takes around 160 secs)
		REPLAY_PREIOD = 153 * 1000;
		SKEW_WEIGHT = 1.7;
		
		SKEW_HIS = new double[] {0.54583648698817977, 0.52947993516897607, 0.56815663293831953, 0.5557219755670012, 0.52260964239135033, 0.44739902767356404, 0.47533003086865105, 0.45937622507886317, 0.48399457802254153, 0.66360143278753403, 0.58691330118081719, 0.45013975792738897, 0.55614430352171396, 0.34360340353334368, 0.35888741193317336, 0.13851197103811203, 0.10603790783238957, 0.098938563934973134, 0.048663315839292044, 0.061324863476499555, 0.057591893429785095, 0.069771773605646389, 0.046696521930276215, 0.24276946676760808, 0.20985150583770779, 0.25760645987487352, 0.2227896175765573, 0.072069462972131904, 0.043896653597605487, 0.06303503141783473, 0.026526247288069151, 0.03707218329685992, 0.15275562275181626, 0.34618641434531994, 0.50478340455614579, 0.53467073070833504, 0.51264284751935474, 0.54267011260625597, 0.3686403537982485, 0.6103426391159632, 0.46500442925835567, 0.40533443175383771, 0.32296178093924649, 0.44129267591938232, 0.50173124122415758, 0.51091277778347521, 0.44410389733241168, 0.52229418511190451, 0.50038718895199608, 0.55355461621281199, 0.57137683019529861};

		NOR1 = new double[] {0.11431982850504784, 0.16403962744154105, 0.13664985820375802, 0.11979080119884182, 0.10904361556468899, 0.12890484809156519, 0.09000959797513651, 0.20092757590991225, 0.12163965422138853, 0.098704914795019236, 0.10699694434253899, 0.12629110157229365, 0.11984961265340985, 0.19803650227232011, 0.19057005739295832, 0.30198432401953773, 0.33130716889894524, 0.25831000767555917, 0.30962212265426359, 0.31294274746059103, 0.33734603985298495, 0.35556743690135023, 0.25968645784649996, 0.25681284978960989, 0.24872505524838465, 0.26476652837404041, 0.25157424919872068, 0.2801560872931102, 0.28069034056867076, 0.33962084179774188, 0.30765980171275503, 0.3342568822033572, 0.23988776192814837, 0.12761011619627757, 0.11061560213240675, 0.1197630771572623, 0.11375136910474964, 0.12883093293781975, 0.17898290692517921, 0.096306732836562325, 0.12565592066341535, 0.12769157766860442, 0.20130560528181576, 0.1784743992402657, 0.12793108011936635, 0.10760895626977858, 0.16724302680442654, 0.13068158521605899, 0.13724322880480133, 0.12380734968409431, 0.11227560483514611};

		NOR2 = new double[] {0.25072053029305041, 0.14736519376532289, 0.1515946411893731, 0.25943874653762661, 0.277497724426776, 0.29938078378627969, 0.30531528141485226, 0.22816581078447482, 0.31384017962862854, 0.15215618408230461, 0.2208884577158475, 0.31676369346913053, 0.21972281898559284, 0.2386997452343822, 0.23927515028870511, 0.35917878382170898, 0.28209867496579888, 0.45196004443352927, 0.41945759034884139, 0.30487933434852332, 0.34090947129169047, 0.2819186594433391, 0.35164926361087129, 0.23340678905637954, 0.29297822266722345, 0.19563290569856778, 0.24873170176951628, 0.31051662941991648, 0.25230094546603382, 0.27462301357379748, 0.35241092475860397, 0.3256754900827063, 0.33077189124239226, 0.33856823948471809, 0.2725579189756871, 0.23416841805528962, 0.24989851542935948, 0.21481820642258162, 0.29680090644880613, 0.19087034174984988, 0.30981401566049332, 0.3646567777150907, 0.35121612621712217, 0.26288580416575641, 0.28492796965287698, 0.26956414286454305, 0.27788983008029972, 0.19566689962565073, 0.24532392437831629, 0.23236976773760959, 0.20712000505523201};

		NOR3 = new double[] {0.089123154213721995, 0.15911524362415988, 0.14359886766854918, 0.065048476696530411, 0.09084901761718453, 0.12431534044859116, 0.12934508974136028, 0.11153038822674974, 0.080525588127441261, 0.085537468335142225, 0.085201296760796311, 0.10680544703118683, 0.10428326483928349, 0.21966034895995404, 0.21126738038516313, 0.20032492112064121, 0.28055624830286624, 0.19079138395593831, 0.2222569711576029, 0.32085305471438608, 0.26415259542553943, 0.29274213004966426, 0.34196775661235262, 0.26701089438640241, 0.2484452162466842, 0.28199410605251829, 0.27690443145520577, 0.33725782031484142, 0.42311206036769, 0.32272111321062602, 0.31340302624057181, 0.3029954444170766, 0.27658472407764317, 0.18763522997368445, 0.11204307433576034, 0.11139777407911318, 0.12370726794653612, 0.11368074803334259, 0.15557583282776632, 0.10248028629762465, 0.099525634417735737, 0.1023172128624672, 0.12451648756181566, 0.11734712067459553, 0.08540970900359908, 0.11191412308220315, 0.11076324578286206, 0.15135733004638588, 0.11704565786488641, 0.09026826636548417, 0.10922755991432331};
	}

	
	static {
		if (NUM_PARTITIONS == -1)
			throw new RuntimeException("it's -1 !!!!");
		
		GLOBAL_COUNTERS = new AtomicInteger[NUM_PARTITIONS];
		for (int i = 0; i < NUM_PARTITIONS; i++)
			GLOBAL_COUNTERS[i] = new AtomicInteger(0);
	}
	
	private static int getNextInsertId(int partitionId) {
		int id = GLOBAL_COUNTERS[partitionId].getAndIncrement();
		int CLIENT_COUNT = NUM_PARTITIONS;
		
		return id * CLIENT_COUNT + nodeId + getStartId(partitionId) + getRecordCount(partitionId);
	}
	
	private static int getStartId(int partitionId) {
		return partitionId * ElasqlYcsbConstants.MAX_RECORD_PER_PART + 1;
	}
	
	private static int getRecordCount(int partitionId) {
		return ElasqlYcsbConstants.RECORD_PER_PART;
	}

	public ElasqlYcsbRealisticbenchmarkParamGen(int nodeId) {
		ElasqlYcsbRealisticbenchmarkParamGen.nodeId = nodeId;
		for (int i = 0; i < NUM_PARTITIONS; i++) {
			int partitionSize = getRecordCount(i);
			latestRandoms[i] = new YcsbLatestGenerator(partitionSize, SKEW_PARAMETER);
		}
	}
	
	@Override
	public TransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}


	@Override
	public Object[] generateParameter() {
		TpccValueGenerator rvg = new TpccValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();
		
		// ================================
		// Decide the types of transactions
		// ================================
		
		boolean isDistributedTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0) ? true : false;
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;
		
		if (NUM_PARTITIONS < 2)
			isDistributedTx = false;
		
		
		//////////////////////////////
//		boolean isChanging = false;
//		
//		if(System.currentTimeMillis()> BENCH_START_TIME + WARMUP_TIME)
//			isChanging = true;
//		
//		int mainPartition = 0;
//		int skewParirion = 0;
//		
//		if (isChanging) {
//			if(rvg.nextDouble() > SKEW_WEIGHT)
//				mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
//			else{
//				mainPartition = skewParirion;
//				System.out.println("Hit on Skew"+skewParirion);
//			}
//		}
//		else {
//			mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
//		}
		
		/////////////////////////////
		
		// =========================================
		// Decide the counts and the main partitions
		// =========================================

		// Choose the main partition
		int mainPartition = 0;
		
		long pt = (System.nanoTime() - Benchmarker.BENCH_START_TIME) / 1_000_000 - WARMUP_TIME;
		int timePoint = (int) (pt / (REPLAY_PREIOD / SKEW_HIS.length));

		if (pt > 0 && timePoint >= 0 && timePoint < SKEW_HIS.length) {
			mainPartition = genDistributionOfPart(timePoint);
			System.out.println("pt " + timePoint);
		}
		else {
			mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
			System.out.println("Choose " + mainPartition);
		}
		
		
		
		latestRandom = latestRandoms[mainPartition];
		
		// Decide counts
		int readCount;
		int localReadCount = 2;
		int remoteReadCount = 2;
		
//		if (isReadWriteTx) 
//			readCount = 1;
		
		if (isDistributedTx)
			readCount = localReadCount+remoteReadCount;
		else
			readCount = localReadCount;
		
		// =====================
		// Generating Parameters
		// =====================
		int[] readRemoteId = new int[remoteReadCount];
		
		if (isDistributedTx) {
			for (int i = 0; i < remoteReadCount; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition, rvg);
				readRemoteId[i] = chooseARecordInMainPartition(remotePartition);
			}
		}
		
		if (isReadWriteTx) {
			int readWriteId = chooseARecordInMainPartition(mainPartition);
			int insertId = getNextInsertId(mainPartition);
			
			// Read count
			paramList.add(readCount);
			
			// Read ids (in integer)
			paramList.add(readWriteId);
			for (int i = 1; i < localReadCount; i++) {
				paramList.add(chooseARecordInMainPartition(mainPartition));
			}
			
			
//			
			
			if (isDistributedTx) {
				for (int i = 0; i < remoteReadCount; i++) {
					paramList.add(readRemoteId[i]);
				}
			}
			
			// Write count
			paramList.add(1);
			
			// Write ids (in integer)
			paramList.add(readWriteId);
			
			// Write values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
			// Insert count
			paramList.add(0);
			
			// Insert ids (in integer)
			paramList.add(insertId);
			
			// Insert values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
		} else {
//			int rec1Id = chooseARecordInMainPartition(mainPartition);
//			int rec2Id = rec1Id;
//			while (rec1Id == rec2Id)
//				rec2Id = chooseARecordInMainPartition(mainPartition);
			
			// Read count
			paramList.add(readCount);
			
			// Read ids (in integer)
//			paramList.add(rec1Id);
//			paramList.add(rec2Id);
			
			for (int i = 0; i < localReadCount; i++) {
				paramList.add(chooseARecordInMainPartition(mainPartition));
			}
			
			if (isDistributedTx) {
				for (int i = 0; i < remoteReadCount; i++) {
					paramList.add(readRemoteId[i]);
				}
			}
			
			// Write count
			paramList.add(0);
			
			// Insert count
			paramList.add(0);
		}
		
		return paramList.toArray(new Object[0]);
	}
	
	private int randomChooseOtherPartition(int mainPartition, TpccValueGenerator rvg) {
		return ((mainPartition + rvg.number(1, NUM_PARTITIONS - 1)) % NUM_PARTITIONS);
	}
	
	private int chooseARecordInMainPartition(int mainPartition) {
		int partitionStartId = getStartId(mainPartition);
		
		return (int) latestRandom.nextValue() + partitionStartId - 1;
	}
	
	private int genDistributionOfPart(int point) {
		LinkedList<Integer> l = new LinkedList<Integer>();
		int len = 100;
		double bot = SKEW_HIS[point] * SKEW_WEIGHT + NOR1[point] + NOR2[point] + NOR3[point];
		for (int i = 0; i < len * SKEW_HIS[point] * SKEW_WEIGHT / bot; i++)
		//for (int i = 0; i < len * SKEW_HIS[point] ; i++)
			l.add(0);
		for (int i = 0; i < len * NOR1[point] / bot; i++)
		//for (int i = 0; i < len * NOR1[point]; i++)
			l.add(1);
		for (int i = 0; i < len * NOR2[point] / bot; i++)
		//for (int i = 0; i < len * NOR2[point]; i++)
			l.add(2);
		for (int i = 0; i < len * NOR3[point] / bot; i++)
		//for (int i = 0; i < len * NOR3[point]; i++)
			l.add(3);
		Collections.shuffle(l);
		return l.getFirst();
	}
}
