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
	private static final long REPLAY_DELAY;
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
		REPLAY_DELAY = 2 * 60000;
		REPLAY_PREIOD = 5 * 60000;
		SKEW_WEIGHT = 1.7;
		//1273811
//		SKEW_HIS = new double[] {0.8942969, 0.8637349, 1.0543365, 1.0595644, 0.8107399, 0.8830483000000001, 0.9356777999999998, 0.8674776000000001, 0.8872107000000001, 0.7769033, 0.8397682999999999, 0.8773308000000001, 0.8020482, 0.9961763000000001, 0.8325499999999999, 0.7915900000000001, 0.8759933000000001, 0.8396931999999999, 0.9146362, 0.8908547, 1.0481805999999998, 0.8387067999999999, 0.9204114000000001, 0.8897334000000001, 0.832522, 0.8913072000000001, 0.9410126999999999, 0.8276217000000001, 0.8283174000000001, 0.9576637999999998, 0.8690277999999999, 1.0652868000000002, 0.8052171999999999, 0.9663147999999999, 0.9101629999999999, 0.9489751000000001, 0.8242339, 0.8878934, 0.8872418, 0.8570107, 0.9619985000000001, 0.9712757, 1.0960445999999997, 0.8438868999999999, 0.9028583, 1.0874264, 0.8906993999999999, 0.8802063999999999, 0.8429618999999999, 0.9078738999999999, 0.880293};
//		//294492126
//		NOR1 = new double[] {0.9977439000000001, 1.3904372, 0.7034450000000001, 0.6486718000000001, 0.7046275000000001, 0.9393995999999999, 0.7551121099999999, 1.0608659, 1.1412546, 1.2601893, 0.9405768999999999, 1.026021, 1.2001202, 1.4586150999999996, 1.1192923000000001, 0.9191623, 1.5034437, 0.9996462999999999, 0.7721815999999999, 1.0604507, 0.9806950000000001, 0.9582467, 0.9086917, 0.9570301, 1.1360560000000002, 1.0104272, 1.1280933000000002, 1.1563974999999997, 1.0965677000000003, 1.0171407, 0.7795034999999999, 0.8553433, 1.1300313999999998, 0.9089345, 0.8414285, 1.063406, 0.9228478, 1.0923479, 1.0805703999999998, 1.1863231, 0.9645467, 1.0356241, 1.1746222, 0.8287662999999998, 0.8801098999999999, 0.7983606999999999, 0.6133703, 0.9260427, 1.0888038999999998, 0.8746558, 0.8071266999999999};
//		//336038688
//		NOR2 = new double[] {0.9470270000000001, 0.47704019999999997, 0.4871308, 1.3810757, 1.1817916, 1.3247356, 1.254593, 0.6863785, 1.1283830000000001, 0.980909, 0.9833391999999999, 1.3527208999999998, 1.1352056, 0.7213945999999999, 0.7569794000000001, 0.7915098, 0.2814728999999999, 0.5028226, 0.5150619000000001, 0.4716136, 0.9560815, 0.7366304, 0.8383615, 0.4746266, 0.4396948, 0.7574846, 0.6382803, 0.42190690000000003, 0.4066436, 0.41499419999999987, 0.4623535, 0.46761499999999995, 0.4317786, 0.5129868, 0.49452219999999997, 0.40743529999999994, 0.40870239999999997, 0.33753640000000007, 0.4011021, 0.4374914000000001, 0.4734546, 0.45684929999999996, 0.443405, 0.5882872, 0.5041362, 0.6854811000000001, 0.8052592000000001, 0.4271247, 0.7584063999999999, 0.8144728999999999, 0.5092363};
//		//400468854
//		NOR3 = new double[] {0.5466003, 0.7268275, 0.9979279, 1.1162697000000001, 0.6584399000000001, 0.9149059, 1.1475183999999998, 1.2059452, 1.4611589, 1.0670647000000002, 1.0977418, 1.0502752, 1.1639811999999998, 1.0537445, 1.029709, 1.0216439, 0.6670013, 0.9534218000000001, 1.2968561, 1.1017564999999998, 1.0829643999999998, 1.2080694000000003, 1.0365077, 0.7643013999999999, 0.6588013, 0.6105387, 0.5539852, 0.7066556, 0.6566987, 0.5870501, 0.6846775000000002, 0.6586031999999999, 0.4840979, 0.7070063999999999, 0.6251591000000001, 1.0252018999999999, 0.6542791, 0.6509206, 0.7696819, 0.9055529999999999, 0.9334879999999999, 0.7192889999999998, 0.9119019000000002, 0.7638613000000001, 0.9868539000000001, 1.0067195, 0.8787867999999999, 0.8065587000000001, 0.6900725000000001, 0.7712076999999999, 0.6551581999999999};
		
		SKEW_HIS = new double[] {0.5553404000000001, 0.564765, 0.6470887, 0.6682555, 0.7419217, 0.6092295000000001, 0.6060315, 0.6393729999999999, 0.5205128, 0.5634679, 0.6555283000000001, 0.7613662, 0.6976283000000002, 0.5473874999999999, 0.5034940999999999, 1.2517355000000001, 0.6403357000000001, 0.6823052, 0.6737227999999998, 0.7056273999999999, 0.7670621999999999, 0.7491587000000001, 0.8073366000000001, 0.6499379, 0.6534885000000001, 0.6548015, 0.7510368, 0.8956467000000001, 0.8416824999999999, 0.6764685999999999, 0.7501254999999999, 0.7833738000000001, 0.6524826, 0.6405521000000001, 0.9454284, 0.9453882, 0.6072046999999998, 0.6614220000000001, 1.3757772999999998, 0.5416057000000001, 0.8636938000000001, 0.5828565000000001, 0.8089731, 0.7647922000000001, 0.8771345000000002, 0.5939931, 0.6741329, 0.6744247, 0.9249373000000002, 0.807158};
		NOR1 = new double[] {0.31446090000000004, 0.3222686, 0.32657349999999996, 0.4005256000000001, 0.3570591, 0.3475938, 0.33744840000000004, 0.37986390000000003, 0.31088999999999994, 0.3806142, 0.3289917, 0.37373589999999995, 0.32028200000000007, 0.38356999999999997, 0.3673673, 0.3750226, 0.3065504, 0.30923999999999996, 0.32232400000000005, 0.3283859999999999, 0.2968930999999999, 0.3258999, 0.3462183, 0.3341347, 0.3666205, 0.4297445, 0.2795414, 0.36210799999999993, 0.33781550000000005, 0.3128013, 0.30781690000000006, 0.3375879, 0.3117801, 0.3239568, 0.3067334, 0.3056747, 0.30994410000000006, 0.32648849999999996, 0.38716149999999994, 0.3555863, 0.31475020000000004, 0.3474443, 0.3284196, 0.29385089999999997, 0.31035480000000004, 0.3120579, 0.30202740000000006, 0.3217631, 0.30944740000000004, 0.3562085, 0.3461975};
		NOR2 = new double[] {0.215605, 0.23742010000000002, 0.19336420000000004, 0.4260123, 0.45877009999999996, 0.21902500000000003, 0.27286699999999997, 0.40122340000000006, 0.31105299999999997, 0.3296846, 0.3462622, 0.3265488, 0.3358781, 0.3052837, 0.35847299999999993, 0.48338460000000005, 0.16701850000000001, 0.1650004, 0.4191396, 0.475498, 0.4320628, 0.414468, 0.43963810000000003, 0.47569579999999995, 0.3803577999999999, 0.1889047, 0.2403421, 0.1752182, 0.40466009999999997, 0.3409314, 0.3497137, 0.41158300000000003, 0.4191663, 0.4030624, 0.4881851, 0.37251179999999995, 0.43291339999999995, 0.3950185, 0.4322034, 0.3839725, 0.4047327, 0.3591682, 0.45517989999999997, 0.5332510000000001, 0.3465093999999999, 0.42916830000000006, 0.42038299999999995, 0.3640057, 0.4770423, 0.6041828, 0.5056678};
		NOR3 = new double[] {0.2976749, 0.4455235, 0.5629878, 0.40646239999999995, 0.3993126, 0.5589346, 0.6142304000000002, 0.3663765, 0.3239055, 0.3587177, 0.34840920000000003, 0.3576851, 0.39230560000000003, 0.4812986999999999, 0.5152008, 0.27374109999999996, 0.36094800000000005, 0.30341450000000003, 0.3245371, 0.4618766, 0.3314369, 0.3481189, 0.5525805, 0.42145540000000004, 0.3854891999999999, 0.42442070000000004, 0.4089837, 0.500489, 0.6484677, 0.4093869, 0.4134379, 0.39488589999999996, 0.4531888, 0.6272618999999999, 0.4085276, 0.3722029, 0.4309172, 0.33480260000000006, 0.3727042, 0.40924530000000003, 0.3393288, 0.3163925, 0.33111809999999997, 0.3153528, 0.30128850000000007, 0.42243150000000007, 0.39028840000000004, 0.4975725, 0.3906208, 0.3244124, 0.42985500000000004};
		
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

		long pt = (System.currentTimeMillis() - BENCH_START_TIME) - REPLAY_DELAY;

		if (pt > 0 && pt < REPLAY_PREIOD) {
			int timePoint = (int) (pt / (REPLAY_PREIOD / SKEW_HIS.length));
			mainPartition = genDistributionOfPart(timePoint);
			System.out.println("Choose " + mainPartition);

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
		double bot = SKEW_HIS[point] * SKEW_WEIGHT + NOR1[point] + NOR2[point] + NOR3[point];
		for (int i = 0; i < len * SKEW_HIS[point] * SKEW_WEIGHT / bot; i++)
			l.add(0);
		for (int i = 0; i < len * NOR1[point] / bot; i++)
			l.add(1);
		for (int i = 0; i < len * NOR2[point] / bot; i++)
			l.add(2);
		for (int i = 0; i < len * NOR3[point] / bot; i++)
			l.add(3);
		Collections.shuffle(l);
		return l.getFirst();

	}
}
