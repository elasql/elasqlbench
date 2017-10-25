package org.elasql.bench.rte.micro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.elasql.bench.micro.ElasqlMicrobenchConstants;
import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.micro.MicroTransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.tpcc.TpccValueGenerator;
import org.vanilladb.bench.util.RandomNonRepeatGenerator;

public class ElasqlMicroRealisticbenchmarkParamGen implements TxParamGenerator {

	private static double[] SKEW_HIS;
	private static double[] NOR1;
	private static double[] NOR2;
	private static double[] NOR3;

	// Transaaction Type
	private static final double DIST_TX_RATE;
	private static final double RW_TX_RATE;
	private static final double SKEW_TX_RATE;
	private static final double LONG_READ_TX_RATE;

	// Read Counts
	private static final int TOTAL_READ_COUNT;
	private static final int LOCAL_HOT_COUNT;
	private static final int REMOTE_HOT_COUNT;
	private static final int REMOTE_COLD_COUNT;
	// The local cold count will be calculated on the fly

	// Access Pattern
	private static final double WRITE_RATIO_IN_RW_TX;
	private static final double HOT_CONFLICT_RATE;
	private static final double SKEW_PERCENTAGE;

	// Data Size
	private static final int DATA_SIZE_PER_PART;
	private static final int HOT_DATA_SIZE_PER_PART;
	private static final int COLD_DATA_SIZE_PER_PART;

	// Shortcuts
	static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;

	// Other parameters
	private static final int RANDOM_SWAP_FACTOR = 20;

	private static final long BENCH_START_TIME;
	private static final long REPLAY_PREIOD;
	private static final long WARMUP_TIME;
	private static final double SKEW_WEIGHT;

	static {
		DIST_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".DIST_TX_RATE", 0.2);
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".RW_TX_RATE", 0.0);
		SKEW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".SKEW_TX_RATE", 0.0);
		LONG_READ_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".LONG_READ_TX_RATE", 0.0);

		TOTAL_READ_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".TOTAL_READ_COUNT", 10);
		LOCAL_HOT_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".LOCAL_HOT_COUNT", 1);
		REMOTE_HOT_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".REMOTE_HOT_COUNT", 0);
		REMOTE_COLD_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".REMOTE_COLD_COUNT", 5);

		WRITE_RATIO_IN_RW_TX = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".WRITE_RATIO_IN_RW_TX", 0.5);
		HOT_CONFLICT_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".HOT_CONFLICT_RATE", 0.01);
		SKEW_PERCENTAGE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".SKEW_PERCENTAGE", 0.2);

		DATA_SIZE_PER_PART = ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
		HOT_DATA_SIZE_PER_PART = (int) (1.0 / HOT_CONFLICT_RATE);
		COLD_DATA_SIZE_PER_PART = DATA_SIZE_PER_PART - HOT_DATA_SIZE_PER_PART;

		BENCH_START_TIME = System.currentTimeMillis();
		WARMUP_TIME = 30 * 1000;
		REPLAY_PREIOD = 153 * 1000;
		SKEW_WEIGHT = 1.7;
		
		
		SKEW_HIS = new double[] {0.2071580978221049, 0.18357740176528745, 0.16901836285314123, 0.08759330719000491, 0.19490935836379111, 0.17529112439346259, 0.3150195502724128, 0.3260536184637004, 0.3936021435393679, 0.28029634434803974, 0.27344054925237715, 0.31386301736696304, 0.30352641901296151, 0.27832381144805179, 0.34448462094521393, 0.25425549817611759, 0.34138590478437342, 0.4715901421421787, 0.28960492403077098, 0.32884148506716504, 0.33692634081345507, 0.37499557895870728, 0.29175924244036822, 0.42511409921418841, 0.34399892106915037, 0.21853248206167075, 0.31073295828130248, 0.29260823259239971, 0.33587876841833464, 0.31662584917719266, 0.3789865920838752, 0.31423315224777959, 0.28581875703379916, 0.27364972803457566, 0.29719976468622061, 0.22189513951659004, 0.39277711385556691, 0.41549943712612958, 0.32489039319441054, 0.35644750139433179, 0.33943991489986181, 0.24386937662072683, 0.1530118077737857, 0.091837874432224284, 0.070565083676403004, 0.15159649498089578, 0.08174079951201961, 0.10956198184515924, 0.086606310654841673, 0.10658797721154753, 0.17339983275732287};

		NOR1 = new double[] {0.15803115778859683, 0.18356881456875887, 0.099222899303539738, 0.17316824532352376, 0.15487757962446333, 0.16820786280001451, 0.11586522306446895, 0.10965853144647716, 0.12186831310289425, 0.096161276213067734, 0.11100527912427423, 0.11451757988836886, 0.12077558915832108, 0.1291031836063547, 0.14415029134902554, 0.10099732909423274, 0.11969339037261797, 0.081270037092440697, 0.11240013343686477, 0.10752810622383502, 0.11038959272102575, 0.08724463115165966, 0.072496624485157035, 0.10781868998952497, 0.1065799405157305, 0.1220390253022945, 0.1253289273968613, 0.12246638443736634, 0.071680788637891527, 0.10247050593470358, 0.099757493983315848, 0.10408206264819335, 0.095597541986200824, 0.10058854687522546, 0.12971961111613112, 0.11216059921371133, 0.10458898971213841, 0.091419880454472011, 0.093843187628739999, 0.11463684099961952, 0.13784007153433131, 0.13538123362142801, 0.18914737828950143, 0.14908104726315466, 0.15474579677203876, 0.2266392726189784, 0.16884964155552631, 0.10979750115221393, 0.19431913396223122, 0.22231237152224789, 0.10683787695241778};

		NOR2 = new double[] {0.22067710530797044, 0.20000575829823142, 0.16939210739488786, 0.1764722052152792, 0.15645390103807877, 0.18993511894257542, 0.14285200472116719, 0.16819565652064836, 0.17130373958606798, 0.1583762730349354, 0.14730991030470239, 0.15340187748198761, 0.17432040681124922, 0.11933427293270074, 0.15404811513467165, 0.1543063344787865, 0.17019828020670591, 0.099700990719792978, 0.14165099123005928, 0.13921983092176526, 0.222041674585058, 0.141540605385092, 0.12990837389552581, 0.18597318848520766, 0.13854641201358645, 0.19247490062225059, 0.16914357943382502, 0.21128843817317292, 0.12959268532128959, 0.17053049650487834, 0.1362705631638364, 0.18487383053712972, 0.16629671665203791, 0.15069619147819391, 0.14442278161848829, 0.12758704283430264, 0.14120541343202611, 0.094921776431579993, 0.14832745071167283, 0.16691952906852645, 0.14629733332120734, 0.15432598108964768, 0.1831065347056931, 0.15462622189278968, 0.1933476804779248, 0.28456356747201045, 0.24700776514040024, 0.22242323825484828, 0.24182400498573484, 0.20538398370235025, 0.24140350104437944};

		NOR3 = new double[] {0.41413363908132783, 0.43284802536772221, 0.56236663044843116, 0.5627662422711921, 0.49375916097366662, 0.46656589386394753, 0.42626322194195093, 0.39609219356917408, 0.31322580377166992, 0.46516610640395722, 0.46824426131864622, 0.41821752526268047, 0.40137758501746823, 0.47323873201289274, 0.35731697257108885, 0.49044083825086321, 0.36872242463630273, 0.34743883004558773, 0.45634395130230493, 0.42441057778723462, 0.33064239188046135, 0.39621918450454108, 0.50583575917894896, 0.28109402231107905, 0.41087472640153283, 0.46695359201378422, 0.39479453488801125, 0.37363694479706094, 0.46284775762248431, 0.41037314838322553, 0.3849853507689725, 0.39681095456689741, 0.4522869843279621, 0.47506553361200499, 0.42865784257916006, 0.53835721843539586, 0.36142848300026864, 0.39815890598781833, 0.43293896846517671, 0.36199612853752222, 0.37642268024459952, 0.46642340866819754, 0.47473427923101968, 0.60445485641183139, 0.58134143907363345, 0.3372006649281154, 0.50240179379205363, 0.55821727874777849, 0.47725055039719222, 0.46571566756385424, 0.47835878924587999};
	}

	private Random random = new Random();

	public ElasqlMicroRealisticbenchmarkParamGen() {

	}

	@Override
	public TransactionType getTxnType() {
		return MicroTransactionType.MICRO;
	}

	// a main application for debugging
	public static void main(String[] args) {
		ElasqlMicroRealisticbenchmarkParamGen executor = new ElasqlMicroRealisticbenchmarkParamGen();

		System.out.println("Parameters:");
		System.out.println("Distributed Tx Rate: " + DIST_TX_RATE);
		System.out.println("Read Write Tx Rate: " + RW_TX_RATE);
		System.out.println("Skew Tx Rate: " + SKEW_TX_RATE);
		System.out.println("Long Read Tx Rate: " + LONG_READ_TX_RATE);
		System.out.println("Total Read Count: " + TOTAL_READ_COUNT);
		System.out.println("Local Hot Count: " + LOCAL_HOT_COUNT);
		System.out.println("Remote Hot Count: " + REMOTE_HOT_COUNT);
		System.out.println("Remote Cold Count: " + REMOTE_COLD_COUNT);
		System.out.println("Write Ratio in RW Tx: " + WRITE_RATIO_IN_RW_TX);
		System.out.println("Hot Conflict Rate: " + HOT_CONFLICT_RATE);
		System.out.println("Skew Percentage: " + SKEW_PERCENTAGE);

		System.out.println("# of items / partition: " + DATA_SIZE_PER_PART);
		System.out.println("# of hot items / partition: " + HOT_DATA_SIZE_PER_PART);
		System.out.println("# of cold items / partition: " + COLD_DATA_SIZE_PER_PART);

		System.out.println();

		for (int i = 0; i < 100; i++) {
			Object[] params = executor.generateParameter();
			 System.out.println(Arrays.toString(params));
		}
	}

	@Override
	public Object[] generateParameter() {
		TpccValueGenerator rvg = new TpccValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();

		// ================================
		// Decide the types of transactions
		// ================================

		boolean isDistributedTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0) ? true
				: false;
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;
		boolean isSkewTx = (rvg.randomChooseFromDistribution(SKEW_TX_RATE, 1 - SKEW_TX_RATE) == 0) ? true : false;
		boolean isLongReadTx = (rvg.randomChooseFromDistribution(LONG_READ_TX_RATE, 1 - LONG_READ_TX_RATE) == 0) ? true
				: false;

		if (NUM_PARTITIONS < 2)
			isDistributedTx = false;

		// =========================================
		// Decide the counts and the main partitions
		// =========================================

		// Choose the main partition
		int mainPartition = 0;

		long pt = (System.currentTimeMillis() - BENCH_START_TIME) - WARMUP_TIME;
		int timePoint = (int) (pt / (REPLAY_PREIOD / SKEW_HIS.length));

		if (pt > 0 && timePoint >= 0 && timePoint < SKEW_HIS.length) {
			mainPartition = genDistributionOfPart(timePoint);
			System.out.println("pt " + timePoint);

		} else {
			if (!isSkewTx) {
				// Uniformly select
				mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
			} else {
				// Avoid to choose the first 1/5 partitions
				// because we need to treat them as remote partitions
				int boundaryPartition = (int) (SKEW_PERCENTAGE * NUM_PARTITIONS) - 1;
				boundaryPartition = (boundaryPartition < 0) ? 0 : boundaryPartition;
				mainPartition = rvg.number(boundaryPartition + 1, NUM_PARTITIONS - 1);

			}
			System.out.println("Choose " + mainPartition);
		}

		// Set read counts
		int totalReadCount = TOTAL_READ_COUNT;
		int localHotCount = LOCAL_HOT_COUNT;
		int remoteHotCount = 0;
		int remoteColdCount = 0;

		// For distributed tx
		if (isDistributedTx) {
			remoteHotCount = REMOTE_HOT_COUNT;
			remoteColdCount = REMOTE_COLD_COUNT;
		}

		// For long read tx
		// 10 times of total records and remote colds
		if (isLongReadTx) {
			totalReadCount *= 10;
			remoteColdCount *= 10;
		}

		// Local cold
		int localColdCount = totalReadCount - localHotCount - remoteHotCount - remoteColdCount;

		// Write Count
		int writeCount = 0;

		if (isReadWriteTx) {
			writeCount = (int) (totalReadCount * WRITE_RATIO_IN_RW_TX);
		}

		// =====================
		// Generating Parameters
		// =====================

		// Set read count
		paramList.add(totalReadCount);

		// Choose local hot records
		chooseHotData(paramList, mainPartition, localHotCount);

		// Choose local cold records
		chooseColdData(paramList, mainPartition, localColdCount);

		// Choose remote records
		if (isDistributedTx) {
			// randomly choose hot data from other partitions
			int[] partitionHotCount = new int[NUM_PARTITIONS];
			partitionHotCount[mainPartition] = 0;

			for (int i = 0; i < remoteHotCount; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition, rvg);
				partitionHotCount[remotePartition]++;
			}

			for (int i = 0; i < NUM_PARTITIONS; i++)
				chooseHotData(paramList, i, partitionHotCount[i]);

			// randomly choose cold data from other partitions
			int[] partitionColdCount = new int[NUM_PARTITIONS];
			partitionColdCount[mainPartition] = 0;

			for (int i = 0; i < remoteColdCount; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition, rvg);
				partitionColdCount[remotePartition]++;
			}

			for (int i = 0; i < NUM_PARTITIONS; i++)
				chooseColdData(paramList, i, partitionColdCount[i]);
		}

		// Set write count
		paramList.add(writeCount);

		// Choose write records
		if (writeCount > 0) {
			// A read-write tx must at least update one hot record.
			paramList.add(paramList.get(1));

			// Choose some item randomly from the rest of items
			Object[] writeIds = randomlyChooseInParams(paramList, 2, totalReadCount + 1, writeCount - 1);
			for (Object id : writeIds)
				paramList.add(id);

			// set the update value
			for (int i = 0; i < writeCount; i++)
				paramList.add(rvg.nextDouble() * 100000);
		}

		return paramList.toArray(new Object[0]);
	}

	/**
	 * @param params
	 * @param startIdx
	 *            the starting index (inclusive)
	 * @param endIdx
	 *            the ending index (exclusive)
	 * @param count
	 * @return
	 */
	private Object[] randomlyChooseInParams(List<Object> params, int startIdx, int endIdx, int count) {
		// Create a clone
		Object[] tmps = new Object[endIdx - startIdx];
		for (int i = 0; i < tmps.length; i++)
			tmps[i] = params.get(startIdx + i);

		// Swap
		for (int times = 0; times < tmps.length * RANDOM_SWAP_FACTOR; times++) {
			int pos = random.nextInt(tmps.length - 1);

			Object tmp = tmps[pos];
			tmps[pos] = tmps[pos + 1];
			tmps[pos + 1] = tmp;
		}

		// Retrieve the first {count} results
		Object[] results = new Integer[count];
		for (int i = 0; i < count; i++)
			results[i] = tmps[i];

		return results;
	}

	private int randomChooseOtherPartition(int mainPartition, TpccValueGenerator rvg) {
		return ((mainPartition + rvg.number(1, NUM_PARTITIONS - 1)) % NUM_PARTITIONS);
	}

	private void chooseHotData(List<Object> paramList, int partition, int count) {
		int minMainPart = partition * DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(HOT_DATA_SIZE_PER_PART);
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPart + tmp;
			paramList.add(itemId);
		}

	}

	private void chooseColdData(List<Object> paramList, int partition, int count) {
		int minMainPartColdData = partition * DATA_SIZE_PER_PART + HOT_DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(COLD_DATA_SIZE_PER_PART);
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPartColdData + tmp;
			paramList.add(itemId);
		}
	}

	private int genDistributionOfPart(int point) {
		LinkedList<Integer> l = new LinkedList<Integer>();
		int len = 100;
		//double bot = SKEW_HIS[point] * SKEW_WEIGHT + NOR1[point] + NOR2[point] + NOR3[point];
		//for (int i = 0; i < len * SKEW_HIS[point] * SKEW_WEIGHT / bot; i++)
		for (int i = 0; i < len * SKEW_HIS[point] ; i++)
			l.add(0);
		//for (int i = 0; i < len * NOR1[point] / bot; i++)
		for (int i = 0; i < len * NOR1[point]; i++)
			l.add(1);
		//for (int i = 0; i < len * NOR2[point] / bot; i++)
		for (int i = 0; i < len * NOR2[point]; i++)
			l.add(2);
		//for (int i = 0; i < len * NOR3[point] / bot; i++)
		for (int i = 0; i < len * NOR3[point]; i++)
			l.add(3);
		Collections.shuffle(l);
		return l.getFirst();
	}
}
