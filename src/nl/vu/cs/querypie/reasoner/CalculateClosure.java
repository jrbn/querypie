package nl.vu.cs.querypie.reasoner;

import ibis.util.ThreadPool;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.querypie.QueryPIE;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.SchemaTerms;
import nl.vu.cs.querypie.storage.disk.RDFStorage;
import nl.vu.cs.querypie.storage.disk.TripleFile;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;
import nl.vu.cs.querypie.storage.memory.MultiValue;
import nl.vu.cs.querypie.storage.memory.SortedCollectionTuples;
import nl.vu.cs.querypie.storage.memory.Triple;
import nl.vu.cs.querypie.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.RemoteCodeExecutor;
import arch.actions.Action;
import arch.actions.SendTo;
import arch.chains.Chain;
import arch.data.types.TBoolean;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class CalculateClosure extends Action {

    private static class Lock {
	boolean finished = false;
    }

    public static class NewClosureCycle implements RemoteCodeExecutor {
	@SuppressWarnings("unchecked")
	@Override
	public void execute(ActionContext context) throws Exception {
	    // Copy the elements in the cache and schema
	    RDFStorage storage = ((RDFStorage) context
		    .getInputLayer(Consts.DEFAULT_INPUT_LAYER_ID));
	    storage.schema2
		    .setCacheSinglePatterns((Map<String, long[]>) context
			    .getObjectFromCache("cache1"));
	    storage.schema2
		    .setCacheLists((Map<String, Map<Long, List<Long>>>) context
			    .getObjectFromCache("cache2"));
	    storage.setClosureTriples((InMemoryTripleContainer) context
		    .getObjectFromCache("all_der"));
	    // Reload the rules
	    Ruleset.getInstance().loadRules(false);
	}
    }

    public static class ReloadRuleset implements RemoteCodeExecutor {
	@Override
	public void execute(ActionContext context) throws Exception {
	    String newList = System
		    .getProperty(Ruleset.RULES_FILE_AFTER_CLOSURE);
	    Ruleset.getInstance().parseRulesetFile(newList);
	    Ruleset.getInstance().loadRules(false);
	}
    }

    public static class ReloadRulesetIncomplete implements RemoteCodeExecutor {
	@Override
	public void execute(ActionContext context) throws Exception {
	    String newList = System
		    .getProperty(Ruleset.RULES_FILE_AFTER_CLOSURE);
	    Ruleset.getInstance().parseRulesetFile(newList);
	    Ruleset.getInstance().loadRules(false);
	    QueryPIE.ENABLE_COMPLETENESS = false;
	}
    }

    static final Logger log = LoggerFactory.getLogger(CalculateClosure.class);

    RDFTerm[] triple = { new RDFTerm(), new RDFTerm(), new RDFTerm() };
    Schema schema;

    Pattern[] queries;
    int current_query;
    int current_iteration;
    boolean new_derived;
    boolean writeToDisk;
    boolean incomplete;

    boolean buildListsPhase;
    boolean closurePhase;

    Set<Triple> firstTriples;
    int previousSizeFirstTriples;
    Set<Triple> restTriples;
    int previousSizeRestTriples;

    InMemoryTripleContainer to_write = new InMemoryTripleContainer();
    InMemoryTripleContainer all_derivation;

    public static Chain applyTo(ActionContext context, Tuple tuple,
	    Chain chain, WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	if (tuple == null) {
	    chain.addAction(CalculateClosure.class.getCanonicalName(), null,
		    -1, false, 0, false, false);
	} else {
	    TBoolean incomplete = new TBoolean();
	    tuple.get(incomplete, 0);
	    chain.addAction(CalculateClosure.class.getCanonicalName(), null,
		    -1, false, 0, false, incomplete.getValue());
	}
	return chain;
    }

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	return applyTo(context, tuple, chain, chainsToResolve, chainsToSend);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {
	// Read the list of queries from either conf or a configuration file
	queries = (Pattern[]) context.getObjectFromCache("list_queries");
	if (queries == null) {
	    // Read the list from a configuration file
	    queries = Ruleset.getInstance().getPrecomputedPatterns();
	    Pattern[] q = new Pattern[queries.length + 2];
	    System.arraycopy(queries, 0, q, 0, queries.length);

	    // Add the first and next patterns to the list of queries
	    Pattern p_first = new Pattern();
	    p_first.setEquivalent(false);
	    p_first.setLocation(null);
	    p_first.p[0].setValue(Schema.ALL_RESOURCES);
	    p_first.p[1].setValue(SchemaTerms.RDF_FIRST);
	    p_first.p[2].setValue(Schema.ALL_RESOURCES);
	    q[q.length - 2] = p_first;

	    Pattern p_rest = new Pattern();
	    p_rest.setEquivalent(false);
	    p_rest.setLocation(null);
	    p_rest.p[0].setValue(Schema.ALL_RESOURCES);
	    p_rest.p[1].setValue(SchemaTerms.RDF_REST);
	    p_rest.p[2].setValue(Schema.ALL_RESOURCES);
	    q[q.length - 1] = p_rest;
	    queries = q;

	    context.putObjectInCache("list_queries", queries);
	}

	// Process the parameters
	current_query = (Integer) params[0];
	new_derived = (Boolean) params[1];
	current_iteration = (Integer) params[2];
	writeToDisk = (Boolean) params[3];
	incomplete = (Boolean) params[4];
	to_write.clear();

	RDFStorage input = (RDFStorage) context
		.getInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
	schema = input.schema2;

	if (current_query == -1) {
	    closurePhase = false;
	    buildListsPhase = false;
	    firstTriples = new HashSet<Triple>();
	    context.putObjectInCache("firstTriples", firstTriples);
	    restTriples = new HashSet<Triple>();
	    context.putObjectInCache("restTriples", restTriples);

	} else {
	    if (current_query < queries.length - 2) {
		buildListsPhase = false;
		closurePhase = true;
	    } else {
		buildListsPhase = true;
		closurePhase = false;

		firstTriples = (Set<Triple>) context
			.getObjectFromCache("firstTriples");
		previousSizeFirstTriples = firstTriples.size();
		restTriples = (Set<Triple>) context
			.getObjectFromCache("restTriples");
		previousSizeRestTriples = restTriples.size();
	    }

	}
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	if (closurePhase) { // Add the triple to a tmp container.
	    if (inputTuple.getNElements() > 3) { // Only inferred triples
		inputTuple.get(triple);
		to_write.addTriple(triple[0], triple[1], triple[2], null);
	    }
	} else if (buildListsPhase) {
	    // Copy the triples into first of rest containers
	    inputTuple.get(triple);
	    Triple t = new Triple();
	    t.subject = triple[0].getValue();
	    t.predicate = triple[1].getValue();
	    t.object = triple[2].getValue();
	    if (current_query != queries.length - 1) {
		// Copy into the first container
		firstTriples.add(t);
	    } else {
		// Copy into the rest container
		restTriples.add(t);
	    }
	}
    }

    private void writeTriplesOnDisk(ActionContext context, Pattern query,
	    Collection<Triple> newTriples) throws IllegalArgumentException,
	    InstantiationException, IllegalAccessException,
	    InvocationTargetException {
	if (newTriples.size() > 0) {
	    TripleFile file = null;
	    for (Triple triple : newTriples) {
		if (file == null) {
		    Constructor<? extends TripleFile> constr = Utils
			    .getTripleFileImplementation(context
				    .getConfiguration());
		    // Determine file name
		    String dir = context.getConfiguration().get(
			    "input.schemaDir")
			    + File.separator + query.getLocation();
		    int i = 0;
		    String outputFile = dir + File.separator + "update-" + i;
		    while (new File(outputFile).exists()) {
			i++;
			outputFile = dir + File.separator + "update-" + i;
		    }
		    file = constr.newInstance(outputFile);
		    file.openToWrite();
		}
		file.write(triple.subject, triple.predicate, triple.object);
	    }
	    if (file != null) {
		file.close();
	    }
	}
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
	    throws Exception {

	RuleBCAlgo.cleanup(context);
	RDFStorage input = (RDFStorage) context
		.getInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
	InMemoryTripleContainer all_derivation = input.getClosureTriples();
	if (all_derivation == null) {
	    all_derivation = new InMemoryTripleContainer(true);
	    input.setClosureTriples(all_derivation);
	}

	// Update the new_derivation flag
	if (closurePhase) {
	    boolean areNewTriples = false;
	    if (to_write.size() > 0
		    || (current_iteration == 0 && queries[current_query]
			    .isEquivalent())) {

		// Process the new triples also checking the equivalent
		/***** CHECKS *****/
		boolean isEquivalent = queries[current_query].isEquivalent();
		int[] pos_vars = queries[current_query].getPositionVars();
		if (isEquivalent
			&& (pos_vars.length != 2 || pos_vars[0] != 0 || pos_vars[1] != 2)) {
		    throw new Exception("Not supported");
		}

		// Write these triples into the corresponding closure
		// directory.
		SortedCollectionTuples existingTriples = schema
			.getVarsPrecomputedPattern(queries[current_query]);

		boolean singleValue = pos_vars.length == 1;
		MultiValue doubleValue = new MultiValue(new long[2]);
		Set<Triple> newTriples = new HashSet<Triple>();

		if (isEquivalent && current_iteration == 0
			&& existingTriples != null) {
		    // Go through all the explicit triples and materialize also
		    // the inverse
		    for (int i = 0; i < existingTriples.size(); ++i) {
			existingTriples.get(doubleValue, i);
			// Invert it
			long box = doubleValue.values[0];
			doubleValue.values[0] = doubleValue.values[1];
			doubleValue.values[1] = box;

			if (!existingTriples.contains(doubleValue)) {
			    Triple swapped_t = new Triple();
			    swapped_t.subject = doubleValue.values[0];
			    swapped_t.predicate = queries[current_query].p[1]
				    .getValue();
			    swapped_t.object = doubleValue.values[1];
			    newTriples.add(swapped_t);
			}
		    }
		}

		for (Triple t : to_write.getTripleSet()) {
		    boolean isNew;
		    if (singleValue) {
			isNew = existingTriples == null
				|| !existingTriples.contains(t.subject);
		    } else {
			doubleValue.values[0] = t.subject;
			doubleValue.values[1] = t.object;
			isNew = existingTriples == null
				|| !existingTriples.contains(doubleValue);
		    }

		    // Add it to the triples to write. If equivalent add an
		    // additional triple
		    if (isNew && newTriples.add(t) && isEquivalent) {
			Triple swapped_t = new Triple();
			swapped_t.subject = t.object;
			swapped_t.predicate = t.predicate;
			swapped_t.object = t.subject;
			newTriples.add(swapped_t);
		    }
		}
		to_write.clear();
		areNewTriples = newTriples.size() > 0;
		if (areNewTriples) {
		    // Replace the triples in the cache of the schema
		    long[] et = schema.readTriples(queries[current_query]);
		    long[] newSize;
		    int i;
		    if (et != null) {
			newSize = new long[et.length + newTriples.size() * 3];
			i = et.length;
			System.arraycopy(et, 0, newSize, 0, et.length);
		    } else {
			newSize = new long[newTriples.size() * 3];
			i = 0;
		    }

		    for (Triple t : newTriples) {
			newSize[i++] = t.subject;
			newSize[i++] = t.predicate;
			newSize[i++] = t.object;

			// Add the triple also to all_derivation
			all_derivation.addTriple(t, null);
		    }

		    // Update the values in the single pattern cache
		    schema.updateCacheSinglePatterns(
			    queries[current_query].getSignature(), newSize);

		    if (writeToDisk) {
			writeTriplesOnDisk(context, queries[current_query],
				newTriples);
		    }

		    log.debug("Query " + queries[current_query]
			    + " has derived " + newTriples.size()
			    + " new triples ");

		}

	    }
	    new_derived |= areNewTriples;

	} else if (buildListsPhase) {

	    // If we have derived more first or rest triples, then we should
	    // repeat a cycle.
	    boolean newLists = firstTriples.size() != previousSizeFirstTriples
		    || restTriples.size() != previousSizeRestTriples;
	    new_derived |= newLists;

	    // (Re)generate the lists
	    if (current_query == queries.length - 1 && newLists) {
		// First determine how many schema patterns use lists.
		List<Pattern> patternsWithQueries = new ArrayList<Pattern>();
		for (Pattern p : queries) {
		    if (p.p[2].getName() != null
			    && p.p[2].getName().equals("listhead")) {
			patternsWithQueries.add(p);
		    }
		}

		// Create a map for the first and rest triples
		Map<Long, Collection<Long>> mFirstTriples = new HashMap<Long, Collection<Long>>();
		for (Triple t : firstTriples) {
		    Collection<Long> col = mFirstTriples.get(t.subject);
		    if (col == null) {
			col = new ArrayList<Long>();
			mFirstTriples.put(t.subject, col);
		    }
		    col.add(t.object);
		}
		Map<Long, Collection<Long>> mRestTriples = new HashMap<Long, Collection<Long>>();
		for (Triple t : restTriples) {
		    Collection<Long> col = mRestTriples.get(t.subject);
		    if (col == null) {
			col = new ArrayList<Long>();
			mRestTriples.put(t.subject, col);
		    }
		    col.add(t.object);
		}

		// For each of them, retrieve the possible values and
		// construct the list
		MultiValue v = new MultiValue(new long[2]);
		for (Pattern p : patternsWithQueries) {
		    if (p.getPositionVars().length != 2) {
			throw new Exception("Not supported");
		    }
		    SortedCollectionTuples values = schema
			    .getVarsPrecomputedPattern(p);
		    if (values != null) {
			Map<Long, List<Long>> map = new HashMap<Long, List<Long>>();
			for (int i = 0; i < values.size(); ++i) {
			    values.get(v, i);
			    // Get the object
			    List<List<Long>> lists = getList(v.values[1],
				    mFirstTriples, mRestTriples, null);
			    if (lists != null) {
				if (lists.size() > 1) {
				    throw new Exception("Not supported");
				} else if (lists.size() == 1) {
				    // Add it to the map
				    map.put(v.values[1], lists.get(0));
				}
			    }
			}

			// Replace the map in the cacheLists of the schema
			schema.updateCacheLists(p.getLocation() + "-list", map);

			if (writeToDisk) {
			    throw new Exception("not (yet) implemented");
			}
		    }
		}
	    }
	}

	if (current_query == queries.length - 1) {
	    log.debug("Finished iteration " + current_iteration
		    + ": new_derived=" + new_derived);

	    // I finished a cycle. Should I do another one?
	    if (new_derived) {
		new_derived = false;

		// Cleanup memory
		context.cleanup();

		// Reindex the triples
		log.debug("Indexing the derivation ...");
		long time = System.currentTimeMillis();
		all_derivation.index();
		log.debug("Finished the indexing :"
			+ (System.currentTimeMillis() - time));

		log.debug("Reloading the rules ...");
		time = System.currentTimeMillis();
		if (context.getNetworkLayer().getNumberNodes() > 1) {
		    // Broadcast everything
		    Map<String, long[]> cache1 = schema
			    .getCacheSinglePatterns();
		    Map<String, Map<Long, List<Long>>> cache2 = schema
			    .getCacheLists();
		    context.putObjectInCache("cache1", cache1);
		    context.putObjectInCache("cache2", cache2);
		    context.putObjectInCache("all_der", all_derivation);
		    long timeBroadcast = System.currentTimeMillis();
		    context.broadcastCacheObjects("cache1", "cache2", "all_der");
		    log.debug("Time to broadcast partial derivation="
			    + (System.currentTimeMillis() - timeBroadcast));

		    // Start new thread loading the rules and waiting until it
		    // finishes
		    final Lock lock = new Lock();
		    ThreadPool.createNew(new Runnable() {
			@Override
			public void run() {
			    try {
				Ruleset.getInstance().loadRules(false);
				synchronized (lock) {
				    lock.finished = true;
				    lock.notify();
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
			}
		    }, "Reload rules");

		    context.executeRemoteCode(NewClosureCycle.class.getName());

		    // Wait until it is finished
		    synchronized (lock) {
			while (!lock.finished)
			    lock.wait();
		    }
		} else {
		    // Reload the rules
		    Ruleset.getInstance().loadRules(false);
		}
		log.debug("Finished reloading the rules :"
			+ (System.currentTimeMillis() - time));

		// Repeat the cycle
		current_query = 0;
		current_iteration++;

	    } else {

		// If there is a new ruleset then we must reload it.
		final String newList = System
			.getProperty(Ruleset.RULES_FILE_AFTER_CLOSURE);
		if (newList != null) {
		    log.debug("Load new ruleset: " + newList);
		    if (context.getNetworkLayer().getNumberNodes() > 1) {

			final Lock lock = new Lock();

			ThreadPool.createNew(new Runnable() {
			    @Override
			    public void run() {
				try {
				    Ruleset.getInstance().parseRulesetFile(
					    newList);
				    Ruleset.getInstance().loadRules(false);
				    QueryPIE.ENABLE_COMPLETENESS = !incomplete;
				    synchronized (lock) {
					lock.finished = true;
					lock.notify();
				    }
				} catch (Exception e) {
				    e.printStackTrace();
				}
			    }
			}, "Reload ruleset");

			if (incomplete) {
			    context.executeRemoteCode(ReloadRulesetIncomplete.class
				    .getName());
			} else {
			    context.executeRemoteCode(ReloadRuleset.class
				    .getName());
			}

			// Wait until it is finished
			synchronized (lock) {
			    while (!lock.finished)
				lock.wait();
			}
		    } else {
			Ruleset.getInstance().parseRulesetFile(newList);
			Ruleset.getInstance().loadRules(false);
			QueryPIE.ENABLE_COMPLETENESS = !incomplete;
		    }
		}

		if (writeToDisk) {
		    throw new Exception("not (yet) implemented!");
		}
		return;
	    }
	}

	Chain newChain = new Chain();
	Tuple tuple = new Tuple();
	
	// ***** LAUNCH THE NEXT QUERY *****
	chain.generateChild(context, newChain);

	newChain.addAction(CalculateClosure.class.getName(), null,
		current_query + 1, new_derived, current_iteration, false,
		incomplete);

	SendTo.applyTo(context, new Tuple(new TString(SendTo.THIS),
		new TBoolean(true)), newChain, null);

	tuple.set(queries[current_query + 1].p);

	RuleBCAlgo.applyTo(context, tuple, newChain);

	newChain.setInputTuple(tuple);
	newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);
	newChains.add(newChain);
	
	schema = null;
	queries = null;
	firstTriples = null;
	restTriples = null;
	all_derivation = null;
    }

    private List<List<Long>> getList(long head,
	    Map<Long, Collection<Long>> firstTriples,
	    Map<Long, Collection<Long>> restTriples, List<Long> existingList)
	    throws Exception {

	if (firstTriples.containsKey(head)) {
	    ArrayList<List<Long>> lists = new ArrayList<List<Long>>();

	    Collection<Long> values = firstTriples.get(head);
	    for (long value : values) {
		// Create a new list
		List<Long> newList = new ArrayList<Long>();
		if (existingList != null) {
		    newList.addAll(existingList);
		}
		newList.add(value);
		// Get the rest
		if (restTriples.containsKey(head)) {
		    Collection<Long> rests = restTriples.get(head);
		    for (long rest : rests) {
			if (rest == SchemaTerms.RDF_NIL) { // List is finished
			    lists.add(newList);
			} else {
			    Collection<List<Long>> l = getList(rest,
				    firstTriples, restTriples, newList);
			    if (l != null) {
				lists.addAll(l);
			    } else {
				log.warn("No firstTriple found for " + head);
			    }
			}
		    }
		} else {
		    log.warn("No rest value found");
		}
	    }

	    return lists;
	}
	return null;
    }
}