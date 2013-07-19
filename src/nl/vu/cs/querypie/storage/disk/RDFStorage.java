package nl.vu.cs.querypie.storage.disk;

import ibis.util.ThreadPool;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import nl.vu.cs.querypie.QueryPIE;
import nl.vu.cs.querypie.dictionary.OnDiskDictionary;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.storage.CompositeTriplePattern;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.SchemaTerms;
import nl.vu.cs.querypie.storage.memory.InMemoryIterator;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;
import nl.vu.cs.querypie.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.chains.Chain;
import arch.data.types.DataProvider;
import arch.data.types.SimpleData;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.storage.Factory;
import arch.utils.Configuration;
import arch.utils.Consts;

public class RDFStorage extends InputLayer {

    protected final EmptyIterator EMPTY_ITR = new EmptyIterator();

    public static final String CACHE_LOCATION = "storage.cache.location";
    public static final String CLEAN_CACHE = "storage.cache.clean";
    public static final String LOCAL_CACHE_LOCATION = "storage.cache.local.location";
    public static final String FILES_INTERFACE = "storage.files.interface";
    public static final String ITERATOR_CLASS = "storage.pattern.iterator";

    static final Logger log = LoggerFactory.getLogger(RDFStorage.class);

    Factory<PatternIterator> factory;
    DataProvider dp = new DataProvider();
    int myPartition;

    OnDiskDictionary dictionary = null;
    protected Map<String, long[][]> indexPartitions = new HashMap<String, long[][]>();
    public Schema schema2;

    protected Map<String, Long> cacheURLs = new HashMap<String, Long>();

    FilesInterface fi;

    public RDFStorage() {
	init();
    }

    int subjectThreshold = Integer.MAX_VALUE;

    protected void init() {
	spo = new Index("spo", pos_spo, true);
	sop = new Index("sop", pos_sop, true);
	pos = new Index("pos", pos_pos, true);
	ops = new Index("ops", pos_ops, true);
	osp = new Index("osp", pos_osp, true);
	pso = new Index("pso", pos_pso, true);
    }

    protected Index spo;
    final int[] pos_spo = { 0, 1, 2 };
    protected Index sop;
    final int[] pos_sop = { 0, 2, 1 };
    protected Index pos;
    final int[] pos_pos = { 1, 2, 0 };
    protected Index ops;
    final int[] pos_ops = { 2, 1, 0 };
    protected Index pso;
    final int[] pos_pso = { 1, 0, 2 };
    protected Index osp;
    final int[] pos_osp = { 2, 0, 1 };

    InMemoryTripleContainer closureTriples = null;

    protected void loadIndex(Configuration conf, String dirIndexes,
	    Index index, String indexType, int myPartition, int nNodes,
	    boolean globallySorted) throws Exception {

	TripleFile[] files = fi.getListFiles(conf, dirIndexes + File.separator
		+ indexType, true);
	TripleFile firstElementsList = Utils.getFirstElementsFile(conf,
		dirIndexes + File.separator + indexType, fi);
	long firstElements[][] = null;
	if (firstElementsList.exists()) {
	    firstElements = new long[files.length][3];
	    firstElementsList.open();
	    for (int i = 0; i < files.length; i++) {
		firstElementsList.next();
		firstElements[i][0] = firstElementsList.getFirstTerm();
		firstElements[i][1] = firstElementsList.getSecondTerm();
		firstElements[i][2] = firstElementsList.getThirdTerm();
	    }
	    firstElementsList.close();
	}

	/* Calculate the number of partitions per node */
	TripleFile lastFile = files[files.length - 1];
	int nAvailablePartitions = Integer.valueOf(lastFile.getName()
		.substring(lastFile.getName().lastIndexOf('-') + 1,
			lastFile.getName().lastIndexOf('_'))) + 1;
	int nPartitionsPerNode = Math.max(nAvailablePartitions / nNodes, 1);

	/* Check whether a cache exists */
	String p = conf.get(CACHE_LOCATION, dirIndexes);
	String subPath = "/" + indexType + "/_cache/" + myPartition + "_"
		+ nPartitionsPerNode;
	String cachePath = p + subPath;

	File cachedir = fi.createFile(cachePath);

	String localCache = conf.get(LOCAL_CACHE_LOCATION, null);
	if (localCache != null) {
	    localCache = localCache + subPath;
	}

	boolean cacheExists = false;
	if (conf.getBoolean(CLEAN_CACHE, false)) {
	    log.debug("deleting dir " + cachedir);
	    Utils.deleteDir(cachedir);
	}

	// useOldGetIndex = conf.getBoolean(OLD_INDEX, false);

	cacheExists = cachedir.exists();

	if (cacheExists) {
	    log.debug("Load index from " + cachePath);
	    index.loadIndexFromCache(localCache, cachePath);
	} else {
	    log.debug("Start writing index to " + cachePath);
	    index.startWritingCache(cachePath);
	}

	int count = 0;

	if (globallySorted) {

	    /* Load the files and the partitions */
	    long[][] partitionTable = new long[nNodes][3];
	    int currentIndex = 0;
	    int currentPartition = 0;
	    for (int i = 0; i < files.length; ++i) {
		TripleFile file = files[i];
		String name = file.getName();
		int partition = Integer.valueOf(name.substring(
			name.lastIndexOf('-') + 1, name.lastIndexOf('_')));
		if (partition == currentPartition
			&& partition < nNodes * nPartitionsPerNode) {
		    if (firstElements != null) {
			partitionTable[currentIndex][0] = firstElements[i][0];
			partitionTable[currentIndex][1] = firstElements[i][1];
			partitionTable[currentIndex][2] = firstElements[i][2];
		    } else {
			// Read the first value and
			// store it
			file.open();
			if (file.next()) {
			    partitionTable[currentIndex][0] = file
				    .getFirstTerm();
			    partitionTable[currentIndex][1] = file
				    .getSecondTerm();
			    partitionTable[currentIndex][2] = file
				    .getThirdTerm();
			}
			file.close();
		    }
		    currentIndex++;
		    currentPartition += nPartitionsPerNode;
		}

		if (!cacheExists) {
		    if (partition >= myPartition * nPartitionsPerNode
			    && (partition < ((myPartition + 1) * nPartitionsPerNode) || myPartition == nNodes - 1)) {
			file.open();
			while (file.next()) {
			    count++;
			    long entry1 = file.getFirstTerm();
			    long entry2 = file.getSecondTerm();
			    long entry3 = file.getThirdTerm();
			    index.writeElement(entry1, entry2, entry3);
			}
			file.close();
		    }
		}
	    }
	    synchronized(indexPartitions) {
		indexPartitions.put(indexType, partitionTable);
	    }
	} else {
	    // The partitions are not globally sorted
	    MergePartitions merge = new MergePartitions();
	    merge.init(myPartition, nPartitionsPerNode, nNodes, files,
		    cacheExists, firstElements);
	    if (!cacheExists) {
		while (merge.hasNext()) {
		    count++;
		    long[] triple = merge.next();
		    index.writeElement(triple[0], triple[1], triple[2]);
		}
	    }
	    synchronized(indexPartitions) {
		indexPartitions.put(indexType, merge.returnPartitionTable());
	    }
	}

	if (!cacheExists) {
	    index.closeWritingCache();
	    log.debug("Loaded triples: " + count);
	}
    }

    int counter;

    @Override
    protected void load(Context context) throws Exception {

	/* Find the correct partition to load */
	myPartition = context.getNetworkLayer().getMyPartition();
	final int nNodes = context.getNetworkLayer().getNumberNodes();
	final Configuration conf = context.getConfiguration();
	String className = conf.get(ITERATOR_CLASS,
		PatternIterator.class.getName());
	Class<? extends PatternIterator> iteratorClass = ClassLoader
		.getSystemClassLoader().loadClass(className)
		.asSubclass(PatternIterator.class);
	factory = new Factory<PatternIterator>(iteratorClass);
	final String indexDir = conf.get("input.dirIndexes", "");
	String p = conf.get("startup.multithread", "");
	boolean multithreadStartup = p.equals("true");

	fi = (FilesInterface) Class.forName(
		context.getConfiguration().get(FILES_INTERFACE,
			FilesInterface.class.getName())).newInstance();
	spo.setFilesInterface(fi);
	sop.setFilesInterface(fi);
	pos.setFilesInterface(fi);
	ops.setFilesInterface(fi);
	osp.setFilesInterface(fi);
	pso.setFilesInterface(fi);

	try {
	    subjectThreshold = conf.getInt("subject.threshold",
		    Integer.MAX_VALUE);
	} catch (NumberFormatException e) {
	    log.warn("Error in subject.threshold configuration parameter", e);
	}

	if (multithreadStartup) {
	    counter = 7;

	    /********* SPO INDEX *********/
	    log.info("Loading index spo ...");
	    ThreadPool.createNew(new Runnable() {
		@Override
		public void run() {
		    try {
			loadIndex(conf, indexDir, spo, "spo", myPartition,
				nNodes, true);
			synchronized (RDFStorage.this) {
			    counter--;
			    RDFStorage.this.notify();
			}
		    } catch (Exception e) {
			log.error("error", e);
		    }
		}
	    }, "Create SPO");

	    /********* SOP INDEX *********/
	    log.info("Loading index sop ...");
	    ThreadPool.createNew(new Runnable() {
		@Override
		public void run() {
		    try {
			loadIndex(conf, indexDir, sop, "sop", myPartition,
				nNodes, true);
			synchronized (RDFStorage.this) {
			    counter--;
			    RDFStorage.this.notify();
			}
		    } catch (Exception e) {
			log.error("error", e);
		    }
		}
	    }, "Create SOP");

	    /********* POS INDEX *********/
	    log.info("Loading index pos ...");
	    ThreadPool.createNew(new Runnable() {
		@Override
		public void run() {
		    try {
			loadIndex(conf, indexDir, pos, "pos", myPartition,
				nNodes, true);
			synchronized (RDFStorage.this) {
			    counter--;
			    RDFStorage.this.notify();
			}
		    } catch (Exception e) {
			log.error("error", e);
		    }
		}
	    }, "Create POS");

	    /********* OPS INDEX *********/
	    log.info("Loading index ops ...");
	    ThreadPool.createNew(new Runnable() {
		@Override
		public void run() {
		    try {
			loadIndex(conf, indexDir, ops, "ops", myPartition,
				nNodes, true);
			synchronized (RDFStorage.this) {
			    counter--;
			    RDFStorage.this.notify();
			}
		    } catch (Exception e) {
			log.error("error", e);
		    }
		}
	    }, "Create OPS");

	    /********* OSP INDEX *********/
	    log.info("Loading index osp ...");
	    ThreadPool.createNew(new Runnable() {
		@Override
		public void run() {
		    try {
			loadIndex(conf, indexDir, osp, "osp", myPartition,
				nNodes, true);
			synchronized (RDFStorage.this) {
			    counter--;
			    RDFStorage.this.notify();
			}
		    } catch (Exception e) {
			log.error("error", e);
		    }
		}
	    }, "Create OSP");

	    /********* PSO INDEX *********/
	    log.info("Loading index pso ...");
	    ThreadPool.createNew(new Runnable() {
		@Override
		public void run() {
		    try {
			loadIndex(conf, indexDir, pso, "pso", myPartition,
				nNodes, true);
			synchronized (RDFStorage.this) {
			    counter--;
			    RDFStorage.this.notify();
			}
		    } catch (Exception e) {
			log.error("error", e);
		    }
		}
	    }, "Create PSO");

	} else {
	    loadIndex(conf, indexDir, osp, "osp", myPartition, nNodes, true);
	    loadIndex(conf, indexDir, pso, "pso", myPartition, nNodes, true);
	    loadIndex(conf, indexDir, ops, "ops", myPartition, nNodes, true);
	    loadIndex(conf, indexDir, pos, "pos", myPartition, nNodes, true);
	    loadIndex(conf, indexDir, spo, "spo", myPartition, nNodes, true);
	    loadIndex(conf, indexDir, sop, "sop", myPartition, nNodes, true);
	}
	// /********* LOAD SCHEMA INFORMATION *********/
	// schema = new Schema();
	// schema.loadSchema(conf, fi);

	/********* LOAD NEW SCHEMA INFORMATION *********/
	Schema.getInstance().init(conf, conf.get("input.schemaDir", ""), fi);
	schema2 = Schema.getInstance();

	/********* LOAD DICTIONARY ***********/
	load_dictionary(context);

	Ruleset.getInstance();

	/*********
	 * LOAD URL-number conversion (to answer SPARQL queries
	 *********/
	loadCacheURLs(conf);

	if (multithreadStartup) {
	    /**** WAIT UNTIL ALL INDEXES ARE CREATED *****/
	    try {
		synchronized (RDFStorage.this) {
		    counter--;
		    while (counter != 0) {
			RDFStorage.this.wait();
		    }
		}
	    } catch (Exception e) {
		log.error("Error in loading", e);
	    }
	}

	// Clean memory
	Runtime.getRuntime().gc();
    }

    public void load_dictionary(Context context) throws IOException {
	String dict_dir = context.getConfiguration().get(Consts.DICT_DIR, null);
	if (dict_dir != null) {
	    log.debug("Loading dictionary ...");
	    dictionary = new OnDiskDictionary();
	    dictionary.load(dict_dir + "/index_sorted.data", dict_dir
		    + "/text.data");
	}
    }

    public String[] getText(long... resources) {
	if (dictionary != null) {
	    return dictionary.getText(resources);
	} else {
	    return null;
	}
    }

    protected void loadCacheURLs(Configuration conf) throws Exception {
	File file = fi.createFile(conf.get("input.cacheURLs", ""));
	if (!file.exists()) {
	    log.warn("Unable to load the cache URL file at " + file.getPath());
	} else {
	    try {
		BufferedReader reader = new BufferedReader(
			new InputStreamReader(fi.createInputStream(file)));
		String line = "";
		while ((line = reader.readLine()) != null) {
		    String[] pair = line.split("\t");
		    cacheURLs.put(pair[1], Long.valueOf(pair[0]).longValue());
		}
		reader.close();
	    } catch (Exception e) {
		log.error("Error loading cache URLs", e);
	    }
	}

	try {
	    Field[] commonURIs = SchemaTerms.class.getFields();
	    for (Field sf : commonURIs) {
		if (sf.getName().startsWith("S_")
			&& sf.getType().equals(String.class)) {
		    String svalue = (String) sf.get(SchemaTerms.class);

		    // Look for the correspondent number
		    String nname = sf.getName().substring(2);
		    boolean found = false;
		    for (Field nf : commonURIs) {
			if (nf.getName().equals(nname)) {
			    found = true;
			    long value = nf.getLong(SchemaTerms.class);
			    cacheURLs.put(svalue, value);
			}
		    }

		    if (!found) {
			log.warn("Numerical value for common URL " + svalue
				+ " not found");
		    }
		}
	    }
	} catch (Exception e) {
	    log.error("Error", e);
	}
    }

    private Index getIndex(long subject, long predicate, long object,
	    Context context, int submissionID) {

	/*
	 * Would it be a good idea to if two of the three are fixed, always use
	 * these two as first and second index? --Ceriel For now, commented out.
	 * if (subject >= 0) { if (predicate >= 0) { return spo; } if (object >=
	 * 0) { return sop; } } else if (predicate >= 0 && object >= 0) { return
	 * ops; // or pos ... one with s as the last index. } /* ... added code
	 * until here.
	 */

	Index index;
	if (subject >= 0
		|| (subject <= RDFTerm.THRESHOLD_VARIABLE && schema2.getSubset(
			subject, context, submissionID).size() < subjectThreshold)
		|| (subject < Schema.ALL_RESOURCES
			&& object <= Schema.ALL_RESOURCES && predicate <= Schema.ALL_RESOURCES)) {

	    if (predicate >= 0) {
		index = spo;
	    } else if (object >= 0) {
		index = sop;
	    } else if (predicate < Schema.ALL_RESOURCES) {
		index = spo;
	    } else if (object < Schema.ALL_RESOURCES) {
		index = sop;
	    } else {
		index = spo;
	    }

	} else if (object >= 0
		|| (object <= RDFTerm.THRESHOLD_VARIABLE && schema2.getSubset(
			object, context, submissionID).size() < subjectThreshold)
		|| (object < Schema.ALL_RESOURCES
			&& subject <= Schema.ALL_RESOURCES && predicate <= Schema.ALL_RESOURCES)) {

	    if (predicate >= 0) {
		index = ops;
	    } else {
		if (subject >= 0) {
		    index = osp;
		} else {
		    if (predicate < Schema.ALL_RESOURCES) {
			index = ops;
		    } else {
			if (subject < Schema.ALL_RESOURCES) {
			    index = osp;
			} else {
			    index = ops;
			}
		    }
		}

	    }
	} else {

	    if (object >= 0) {
		index = pos;
	    } else {
		if (subject >= 0) {
		    index = pso;
		} else {
		    if (subject < Schema.ALL_RESOURCES) {
			index = pso;
		    } else {
			index = pos;
		    }
		}
	    }
	}

	return index;
    }

    public PatternIterator getIterator(long[] t, InMemoryTripleContainer input,
	    ActionContext context) throws Exception {
	PatternIterator itr = factory.get();
	Index index = getIndex(t[0], t[1], t[2], context.getGlobalContext(),
		context.getSubmissionID());

	if (log.isDebugEnabled()) {
	    log.debug("Index chosen for pattern " + Arrays.toString(t) + ": "
		    + index.getName());
	}

	long minValue = Long.MIN_VALUE;
	long maxValue = Long.MAX_VALUE;

	long[][] part = indexPartitions.get(index.getName());
	minValue = part[myPartition][0];
	if (myPartition < part.length - 1)
	    maxValue = part[myPartition + 1][0];
	itr.init(index, schema2, t, context, minValue, maxValue, input);
	return itr;
    }

    @Override
    public TupleIterator getIterator(Tuple tuple, ActionContext context) {
	try {
	    if (QueryPIE.COUNT_LOOKUPS) {
		context.incrCounter("lookups", 1);
	    }

	    RDFTerm token1 = (RDFTerm) dp.get(RDFTerm.DATATYPE);
	    RDFTerm token2 = (RDFTerm) dp.get(RDFTerm.DATATYPE);
	    RDFTerm token3 = (RDFTerm) dp.get(RDFTerm.DATATYPE);
	    tuple.get(token1, token2, token3);
	    PatternIterator itr = null;

	    if (token1.getValue() != Schema.SCHEMA_SUBSET) {
		long[] t = new long[3];
		t[0] = token1.getValue();
		t[1] = token2.getValue();
		t[2] = token3.getValue();

		InMemoryTripleContainer input = (InMemoryTripleContainer) context
			.getObjectFromCache("inputIntermediateTuples");
		if (input != null) {
		    TupleIterator first = InMemoryIterator.getIterator(context,
			    input, t[0], t[1], t[2]);
		    if (input.containsQuery(t[0], t[1], t[2], context)) {
			return first;
		    } else {
			itr = getIterator(t, input, context);
			if (closureTriples != null) {
			    TupleIterator second = InMemoryIterator
				    .getIterator(context, closureTriples, t[0],
					    t[1], t[2]);
			    dp.release(token1);
			    dp.release(token2);
			    dp.release(token3);
			    return new CompositeTriplePattern(first, second,
				    itr);
			} else {
			    dp.release(token1);
			    dp.release(token2);
			    dp.release(token3);
			    return new CompositeTriplePattern(first, itr);
			}
		    }
		} else {
		    itr = getIterator(t, null, context);
		    if (closureTriples != null) {
			TupleIterator second = InMemoryIterator.getIterator(
				context, closureTriples, t[0], t[1], t[2]);
			dp.release(token1);
			dp.release(token2);
			dp.release(token3);
			return new CompositeTriplePattern(second, itr);
		    }
		}
		// }

	    } else {
		itr = factory.get();
		itr.init(context);
	    }

	    dp.release(token1);
	    dp.release(token2);
	    dp.release(token3);

	    return itr;

	} catch (Exception e) {
	    log.error("Error", e);
	}

	return null;
    }

    public InMemoryTripleContainer getClosureTriples() {
	return closureTriples;
    }

    public void setClosureTriples(InMemoryTripleContainer closureTriples) {
	this.closureTriples = closureTriples;
    }

    Random r = new Random();

    @Override
    public int[] getLocations(Tuple tuple, Chain chain, Context context) {
	try {
	    SimpleData token1 = dp.get(tuple.getType(0));
	    SimpleData token2 = dp.get(tuple.getType(1));
	    SimpleData token3 = dp.get(tuple.getType(2));
	    tuple.get(token1, token2, token3);

	    long first = ((RDFTerm) token1).getValue();
	    long second = ((RDFTerm) token2).getValue();
	    long third = ((RDFTerm) token3).getValue();

	    dp.release(token1);
	    dp.release(token2);
	    dp.release(token3);

	    int[] range = new int[2];
	    if (first == Schema.SCHEMA_SUBSET) {
		range[1] = range[0] = r.nextInt(context.getNetworkLayer()
			.getNumberNodes());
		return range;
	    }

	    Index index = getIndex(first, second, third, context,
		    chain.getSubmissionId());

	    long[][] partitionTable = indexPartitions.get(index.getName());
	    long firstEntry;
	    if (index == ops || index == osp) {
		firstEntry = third;
	    } else if (index == pso || index == pos) {
		firstEntry = second;
	    } else {
		firstEntry = first;
	    }

	    // If the first entry is a variable, broadcast.
	    if (firstEntry <= RDFTerm.THRESHOLD_VARIABLE) { // Broadcast
		range[0] = 0;
		range[1] = context.getNetworkLayer().getNumberNodes() - 1;
		return range;
	    }
	    // Now, the first entry is always greater than 0.
	    // Search which partition it is with naive search
	    int startPartition = 0;
	    int endPartition = partitionTable.length - 1;
	    for (int i = 0; i <= endPartition; ++i) {
		if (partitionTable[i][0] > firstEntry) {
		    endPartition = i - 1;
		    break;
		} else if (partitionTable[i][0] < firstEntry) {
		    startPartition = i;
		}
	    }

	    if (startPartition < endPartition) {
		// Check the second entry
		long secondEntry;
		if (index == sop || index == pos) {
		    secondEntry = third;
		} else if (index == spo || index == ops) {
		    secondEntry = second;
		} else {
		    secondEntry = first;
		}
		if (secondEntry >= 0) {
		    // Repeat the same with the second entry
		    for (int i = startPartition; i <= endPartition; ++i) {
			if (partitionTable[i][1] > secondEntry
				&& partitionTable[i][0] == firstEntry) {
			    endPartition = i - 1;
			    break;
			} else if (partitionTable[i][1] < secondEntry
				&& partitionTable[i][0] == firstEntry) {
			    startPartition = i;
			}
		    }
		}
	    }

	    range[0] = startPartition;
	    range[1] = endPartition;
	    return range;

	} catch (Exception e) {
	    log.error("Error", e);
	}

	return null;
    }

    // public Schema getSchema() {
    // return schema;
    // }

    public Map<String, Long> getCacheURLs() {
	return cacheURLs;
    }

    long chainId = -1;

    @Override
    public void releaseIterator(TupleIterator itr, ActionContext context) {
	if (itr instanceof PatternIterator) {
	    factory.release((PatternIterator) itr);
	} else if (itr instanceof CompositeTriplePattern) {
	    CompositeTriplePattern itr2 = (CompositeTriplePattern) itr;
	    if (itr2.third != null) {
		factory.release((PatternIterator) itr2.third);
	    } else {
		factory.release((PatternIterator) itr2.second);
	    }
	}
    }
}
