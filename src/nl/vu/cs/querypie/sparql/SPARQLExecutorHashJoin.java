package nl.vu.cs.querypie.sparql;

import java.util.Set;
import java.util.TreeSet;

import nl.vu.cs.querypie.QueryPIE;
import nl.vu.cs.querypie.reasoner.RuleBCAlgo;
import nl.vu.cs.querypie.reasoner.RuleGetPattern;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoner.rules.executors.TreeExpander;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.MultiValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.actions.FilterDuplicates;
import arch.actions.PutIntoBucket;
import arch.actions.SendTo;
import arch.actions.joins.HashJoin;
import arch.chains.Chain;
import arch.data.types.SimpleData;
import arch.data.types.TBoolean;
import arch.data.types.TInt;
import arch.data.types.TLong;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.TupleComparator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class SPARQLExecutorHashJoin extends Action {

    static final Logger log = LoggerFactory
	    .getLogger(SPARQLExecutorHashJoin.class);

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	inputChain.addAction(this, null, 0);
	return inputChain;
    }

    private int step = 0;
    private final RDFTerm[] t2 = { new RDFTerm(), new RDFTerm(), new RDFTerm() };

    // Variables used for step 3
    RDFTerm value = new RDFTerm();
    Set<Long> customSet = null;
    int posVariable = 0;
    long nameVariable = 0;

    // Variables used for step 4
    int bucketID;
    int parentBucketID;
    long[] inputPattern = new long[3];
    long[] bucketPattern;
    long[] remainingPattern;
    boolean first;

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) {
	step = (Integer) params[0];

	if (step == 3) {
	    customSet = new TreeSet<Long>();
	    posVariable = (Integer) params[1];
	    nameVariable = (Long) params[2];
	} else if (step == 4) {
	    first = true;
	    bucketID = (Integer) params[1];
	    parentBucketID = (Integer) params[2];
	    inputPattern[0] = (Long) params[3];
	    inputPattern[1] = (Long) params[4];
	    inputPattern[2] = (Long) params[5];
	    int sizeBucket = (Integer) params[6];
	    bucketPattern = new long[sizeBucket];
	    for (int i = 0; i < bucketPattern.length; i++) {
		bucketPattern[i] = (Long) params[7 + i];
	    }
	    int sizeRem = (Integer) params[sizeBucket + 7];
	    if (sizeRem > 0) {
		remainingPattern = new long[sizeRem];
		for (int i = 0; i < sizeRem; ++i) {
		    remainingPattern[i] = (Long) params[sizeBucket + 8 + i];
		}
	    } else {
		remainingPattern = new long[0];
	    }
	} else if (step == 5) {
	    first = true;
	}
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output,
	    WritableContainer<Chain> newChains,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	if (step == 3) {
	    context.putObjectInCache(nameVariable, customSet);
	    log.debug("Put into " + nameVariable + " a set of "
		    + customSet.size() + " elements");
	    customSet = null;
	}
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {

	switch (step) {
	case 0:
	    step0(inputTuple, context, remainingChain, chainsToResolve);
	    break;
	case 3:
	    step3(inputTuple, output);
	    break;
	case 4:
	    step4(inputTuple, context, remainingChain, chainsToResolve,
		    chainsToProcess);
	    break;
	case 5:
	    step5(inputTuple, output);
	    break;
	}
    }

    private int calculateJoinIndexes(long[] t1, long[] t2, int[] index1,
	    int[] index2) {
	// Calculate the joins that need to be done
	int joinIndexLength = 0;
	for (int i = 0; i < t1.length; ++i) {
	    if (t1[i] < 0) {
		for (int m = 0; m < t2.length; ++m) {
		    if (t2[m] == t1[i]) {
			// Found join point
			index1[joinIndexLength] = i;
			index2[joinIndexLength] = m;
			joinIndexLength++;
		    }
		}
	    }
	}

	return joinIndexLength;
    }

    public void step0(Tuple inputTuple, ActionContext context,
	    Chain remainingChain, WritableContainer<Chain> newChains) {
	// Read the tuple in input. Should be a TString containing the
	// sparql query
	try {
	    Chain newChain = new Chain();
	    Tuple tuple = new Tuple();

	    log.info("Enter step 0");

	    long[] serializedJoin = new long[inputTuple.getNElements()];
	    // Fill it from the tuple
	    TLong number = new TLong();
	    for (int i = 0; i < inputTuple.getNElements(); ++i) {
		inputTuple.get(number, i);
		serializedJoin[i] = number.getValue();
	    }

	    if (serializedJoin.length > 3) {

		/***** START CHAIN *****/
		remainingChain.generateChild(context, newChain);

		/***** IF THERE ARE ELEMENTS GENERATE OTHER JOIN *****/
		int bucketID = context.getNewBucketID();
		Object[] params = new Object[5 + serializedJoin.length];
		params[0] = 4; // Step 4
		params[1] = bucketID; // Bucket ID where to read
				      // the second
				      // pattern
		params[2] = context.getNewBucketID();

		// Pattern in input
		params[3] = serializedJoin[3];
		params[4] = serializedJoin[4];
		params[5] = serializedJoin[5];

		// Tuples in the bucket
		params[6] = 3;
		params[7] = serializedJoin[0];
		params[8] = serializedJoin[1];
		params[9] = serializedJoin[2];

		// Remaining
		params[10] = serializedJoin.length - 6;
		for (int i = 0; i < serializedJoin.length - 6; ++i) {
		    params[11 + i] = serializedJoin[6 + i];
		}
		newChain.addAction(this, null, params);

		/*****
		 * SEND THE FIRST TRIPLE TO ONE NODE THAT IS USED AS
		 * COORDINATION POINT
		 *****/
		tuple.set(new TString(SendTo.THIS), new TBoolean(true),
			new TInt(context.getNewBucketID()), new TBoolean(false));
		SendTo.applyTo(context, tuple, newChain, null);

		/***** FORWARD ONLY THE FIRST TUPLE *****/
		newChain.addAction(this, null, 5);

		/***** COPY THE TUPLES IN A LOCAL BUCKET *****/
		tuple.set(new TInt(bucketID));
		PutIntoBucket.applyTo(context, tuple, newChain, null);

		/***** PUT THE KEYS INTO A CUSTOM SET *****/
		int sizeBucket = (Integer) params[6];
		long[] bucketPattern = new long[sizeBucket];
		for (int i = 0; i < bucketPattern.length; i++) {
		    bucketPattern[i] = (Long) params[7 + i];
		}
		int pos = -1;
		long name = 0;
		boolean found = false;
		for (int i = 0; i < sizeBucket && !found; ++i) {
		    if (bucketPattern[i] < -1) {
			for (int j = 0; j < 3; ++j) {
			    if (bucketPattern[i] == serializedJoin[j]) {
				pos = i;
				name = bucketPattern[i];
				found = true;
			    }
			}

		    }
		}
		newChain.addAction(this, null, 3, pos, name);

		/***** FILTER THE DUPLICATES *****/
		FilterDuplicates.applyTo(context, tuple, newChain, null);

		/***** SEND TO ALL THE NODES *****/
		tuple.set(new TString(SendTo.ALL), new TBoolean(true),
			new TInt(context.getNewBucketID()),
			new TBoolean(false),
			new TString(TupleComparator.class.getName()));
		newChain = SendTo.applyTo(context, tuple, newChain, null);

		/***** READ *****/
		for (int m = 0; m < 3; ++m) {
		    if (bucketPattern[m] < 0) {
			t2[m].setValue(Schema.ALL_RESOURCES);
		    } else {
			t2[m].setValue(bucketPattern[m]);
		    }
		}
		tuple.set(t2);

		if (QueryPIE.ENABLE_COMPLETENESS) {
		    RemoveLastElement.applyTo(context, null, newChain);
		    RuleBCAlgo.applyTo(context, tuple, newChain);
		} else {
		    RuleGetPattern.applyTo(context, tuple, newChain);
		}

		newChain.setInputTuple(tuple);
		newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);
		newChains.add(newChain);

	    } else {

		/***** START CHAIN *****/
		remainingChain.generateChild(context, newChain);

		/***** READ *****/
		for (int m = 0; m < 3; ++m) {
		    if (serializedJoin[m] < 0) {
			t2[m].setValue(Schema.ALL_RESOURCES);
		    } else {
			t2[m].setValue(serializedJoin[m]);
		    }
		}
		tuple.set(t2);

		if (QueryPIE.ENABLE_COMPLETENESS) {
		    RemoveLastElement.applyTo(context, null, newChain);
		    RuleBCAlgo.applyTo(context, tuple, newChain);
		} else {
		    RuleGetPattern.applyTo(context, tuple, newChain);
		}

		newChain.setInputTuple(tuple);
		newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);
		newChains.add(newChain);
	    }
	} catch (Exception e) {
	    log.error("Error processing query", e);
	}
    }

    public void step3(Tuple inputTuple, WritableContainer<Tuple> output)
	    throws Exception {
	inputTuple.get(value, posVariable);
	customSet.add(value.getValue());
	output.add(inputTuple);
    }

    public void step4(Tuple inputTuple, ActionContext context,
	    Chain remainingChain, WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {

	if (!first) {
	    return;
	}
	first = false;

	log.info("Enter in step 4");

	Chain newChain = new Chain();
	Tuple tuple = new Tuple();

	/***** START NEW CHAIN *****/
	remainingChain.generateChild(context, newChain);

	if (remainingPattern.length > 0) {

	    // Send the results to all the nodes
	    /***** IF THERE ARE ELEMENTS GENERATE OTHER JOIN *****/
	    Object[] params = new Object[7 + bucketPattern.length
		    + remainingPattern.length];
	    params[0] = 4; // Step 4
	    params[1] = parentBucketID;
	    params[2] = context.getNewBucketID(); // Bucket ID where
						  // to
						  // store the
						  // eventual
						  // result

	    // Pattern in input
	    params[3] = remainingPattern[0];
	    params[4] = remainingPattern[1];
	    params[5] = remainingPattern[2];

	    // Tuples in the bucket
	    int sizeBucket = bucketPattern.length + 3 - 1;
	    params[6] = sizeBucket;
	    for (int i = 0; i < bucketPattern.length; ++i) {
		params[7 + i] = bucketPattern[i];
	    }
	    int j = 0;
	    for (int i = 0; i < 3; ++i) {
		boolean found = false;
		for (long b : bucketPattern) {
		    if (inputPattern[i] == b
			    && inputPattern[i] <= RDFTerm.THRESHOLD_VARIABLE) {
			found = true;
			break;
		    }
		}

		if (!found) {
		    params[7 + bucketPattern.length + j++] = inputPattern[i];
		}
	    }

	    // Remaining
	    params[7 + sizeBucket] = remainingPattern.length - 3;
	    for (int i = 0; i < remainingPattern.length - 3; ++i) {
		params[8 + sizeBucket + i] = remainingPattern[3 + i];
	    }
	    newChain.addAction(this, null, params);

	    /*****
	     * SEND THE FIRST TRIPLE TO ONE NODE THAT IS USED AS COORDINATION
	     * POINT
	     *****/
	    tuple.set(new TString(SendTo.THIS), new TBoolean(true), new TInt(
		    context.getNewBucketID()), new TBoolean(false));
	    newChain = SendTo.applyTo(context, tuple, newChain, null);

	    /***** FORWARD ONLY THE FIRST TUPLE *****/
	    newChain.addAction(this, null, 5);

	    /***** COPY THE TUPLES IN A LOCAL BUCKET *****/
	    tuple.set(new TInt(parentBucketID));
	    newChain = PutIntoBucket.applyTo(context, tuple, newChain, null);

	    /***** PUT THE KEYS INTO A CUSTOM SET *****/
	    int pos = -1;
	    long name = 0;
	    boolean found = false;
	    for (int i = 0; i < 3 && !found; ++i) {
		name = remainingPattern[i];
		for (int m = 0; m < sizeBucket && !found; m++) {
		    if (name == ((Long) params[7 + m]).longValue()) {
			pos = m;
			found = true;
		    }
		}
	    }
	    if (!found) {
		throw new Exception("Cannot do the join!");
	    }
	    newChain.addAction(this, null, 3, pos, name);

	    /***** SEND TO ALL THE NODES *****/
	    tuple.set(new TString(SendTo.ALL), new TBoolean(true), new TInt(
		    context.getNewBucketID()), new TBoolean(false));
	    SendTo.applyTo(context, tuple, newChain, null);
	}

	/***** PERFORM THE JOIN *****/
	int[] jf1 = new int[3];
	int[] jf2 = new int[3];
	int len = calculateJoinIndexes(bucketPattern, inputPattern, jf1, jf2);
	SimpleData[] mjparams = new SimpleData[2 + len * 2];
	mjparams[0] = new TInt(bucketID); // Bucket
	mjparams[1] = new TInt(len); // Length join
	for (int i = 0; i < len; ++i) {
	    mjparams[2 + i] = new TInt(jf1[i]);
	    mjparams[2 + len + i] = new TInt(jf2[i]);
	}
	tuple.set(mjparams);
	HashJoin.applyTo(context, tuple, newChain, null);

	/***** CHECK WHETHER WE SHOULD APPLY INCREMENTAL EXECUTION *****/
	boolean canApplyIncrementalReasoning = false;
	// If there is another variable that is not part of the
	// join set we cannot apply this method
	if (len == 1) {
	    canApplyIncrementalReasoning = true;
	    for (int i = 0; i < inputPattern.length; ++i) {
		if (inputPattern[i] < 0 && i != jf2[0]) {
		    canApplyIncrementalReasoning = false;
		}
	    }
	}

	int bucket = 0;
	if (canApplyIncrementalReasoning) {
	    log.info("Applying incremental execution");

	    /***** CHECK WHETHER WE NEED TO REPEAT THE CYCLE *****/
	    bucket = context.getNewBucketID();
	    SPARQLCheckBindings.applyTo(context, new Tuple(new TInt(bucket),
		    new TLong(inputPattern[0]), new TLong(inputPattern[1]),
		    new TLong(inputPattern[2]), new TInt(jf2[0])), newChain,
		    null);

	    /***** SEND THE BINDINGS TO ONE BUCKET *****/
	    tuple.set(new TString(SendTo.THIS), new TBoolean(false), new TInt(
		    bucket), new TBoolean(true));
	    SendTo.applyTo(context, tuple, newChain, null);
	}

	/***** FILTER THE DUPLICATES *****/
	FilterDuplicates.applyTo(context, tuple, newChain, null);

	/***** SEND TO MULTIPLE NODE *****/
	tuple.set(new TString(SendTo.MULTIPLE), new TBoolean(true), new TInt(
		context.getNewBucketID()), new TBoolean(false), new TString(
		TupleComparator.class.getName()));
	SendTo.applyTo(context, tuple, newChain, null);

	/***** PARTITION THE TUPLES ******/
	TriplePartitioner.applyTo(context, null, newChain, null);

	/***** READ TRIPLE IN INPUT *****/
	SimpleData[] t = t2;
	for (int m = 0; m < 3; ++m) {
	    if (inputPattern[m] < 0) {
		boolean join = false;
		for (int j = 0; j < len; ++j) {
		    if (m == jf2[j]) {
			join = true;
			break;
		    }
		}
		if (join) {
		    ((RDFTerm) t[m]).setValue(inputPattern[m]);
		} else {
		    ((RDFTerm) t[m]).setValue(Schema.ALL_RESOURCES);
		}
	    } else {
		((RDFTerm) t[m]).setValue(inputPattern[m]);
	    }
	}
	tuple.set(t);

	RuleBCAlgo.cleanup(context);
	if (QueryPIE.ENABLE_COMPLETENESS && !canApplyIncrementalReasoning) {
	    RemoveLastElement.applyTo(context, null, newChain);
	    RuleBCAlgo.applyTo(context, tuple, newChain);
	} else {
	    RuleGetPattern.applyTo(context, tuple, newChain);
	}

	newChain.setInputTuple(tuple);
	newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);

	if (canApplyIncrementalReasoning) {
	    // Apply rules of the first type
	    Ruleset ruleset = Ruleset.getInstance();
	    int i = 0;
	    boolean first = true;
	    TreeExpander.RuleChildren children = null;
	    for (Rule1 rule : ruleset.getAllActiveFirstTypeRules()) {
		if (!TreeExpander.checkHead(rule, t2, context)) {
		    continue;
		}
		children = TreeExpander.applyRuleFirstType(rule, t2);
		if (children != null) {
		    break;
		}
		i++;
	    }

	    // Apply rules of the second type
	    if (children == null) {
		first = false;
		i = 0;
		RDFTerm[] supportTriple = { new RDFTerm(), new RDFTerm(),
			new RDFTerm() };
		MultiValue k1 = new MultiValue(new long[1]);
		MultiValue k2 = new MultiValue(new long[2]);

		for (Rule2 rule : ruleset.getAllActiveSecondTypeRules()) {
		    if (!TreeExpander.checkHead(rule, t2, context)) {
			continue;
		    }
		    children = TreeExpander.applyRuleWithGenerics(rule,
			    rule.GENERICS_STRATS, t2, context, supportTriple,
			    k1, k2);
		    if (children != null) {
			break;
		    }
		    i++;
		}
	    }

	    if (children == null) {
		context.putObjectInCache("sparql.incr.active", false);
		chainsToResolve.add(newChain);
	    } else {
		context.putObjectInCache("sparql.incr.active", true);
		context.putObjectInCache("sparql.incr.isFirst", first);
		context.putObjectInCache("sparql.incr.currentRule", i);
		context.putObjectInCache("sparql.incr.children", children);

		boolean originalCompleteness = QueryPIE.ENABLE_COMPLETENESS;
		QueryPIE.ENABLE_COMPLETENESS = false;
		TreeExpander.generate_new_chains(newChain, children,
			chainsToSend, context, tuple, new Chain(),
			TreeExpander.CONCURRENT_ACTIONS_BFS);
		QueryPIE.ENABLE_COMPLETENESS = originalCompleteness;
		chainsToSend.add(newChain);
	    }

	} else {
	    chainsToResolve.add(newChain);
	}
    }

    public void step5(Tuple inputTuple, WritableContainer<Tuple> output)
	    throws Exception {
	if (!first) {
	    return;
	}
	first = false;
	output.add(inputTuple);
    }

}
