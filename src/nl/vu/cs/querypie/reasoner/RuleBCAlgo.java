package nl.vu.cs.querypie.reasoner;

import java.util.List;

import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.actions.FilterDuplicates;
import arch.actions.SendTo;
import arch.chains.Chain;
import arch.data.types.TBoolean;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.TupleComparator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class RuleBCAlgo extends Action {

    static final Logger log = LoggerFactory.getLogger(RuleBCAlgo.class);

    RDFTerm[] triple = { new RDFTerm(), new RDFTerm(), new RDFTerm() };
    Object[] params;

    InMemoryTripleContainer outputContainer = null;

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	return applyTo(context, tuple, chain);
    }

    public static Chain applyTo(ActionContext context, Tuple tuple, Chain chain)
	    throws Exception {

	RDFTerm term1 = (RDFTerm) context.getDataProvider().get(
		RDFTerm.DATATYPE);
	RDFTerm term2 = (RDFTerm) context.getDataProvider().get(
		RDFTerm.DATATYPE);
	RDFTerm term3 = (RDFTerm) context.getDataProvider().get(
		RDFTerm.DATATYPE);

	tuple.get(term1, term2, term3);

	chain.addAction(RuleBCAlgo.class.getName(), tuple, term1.getValue(),
		term2.getValue(), term3.getValue());

	AddIntermediateTriples.applyTo(context, term1.getValue(),
		term2.getValue(), term3.getValue(), chain);

	FilterDuplicates.applyTo(context, null, chain, null);

	SendTo.applyTo(context, new Tuple(new TString(SendTo.THIS),
		new TBoolean(true), new TInt(context.getNewBucketID()),
		new TString(TupleComparator.class.getName())), chain, null);

	context.getDataProvider().release(term1);
	context.getDataProvider().release(term2);
	context.getDataProvider().release(term3);

	return chain;
    }

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {
	this.params = params;

	outputContainer = (InMemoryTripleContainer) context
		.getObjectFromCache("outputSoFar");
	if (outputContainer == null) {
	    outputContainer = new InMemoryTripleContainer();
	    context.putObjectInCache("outputSoFar", outputContainer);
	}
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	inputTuple.get(triple);
	if (!outputContainer.containsTriple(triple)) {
	    output.add(inputTuple);
	    outputContainer.addTriple(triple[0], triple[1], triple[2], null);
	}
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output,
	    WritableContainer<Chain> newChains,
	    WritableContainer<Chain> chainsToSend) throws Exception {

	// Get local data structures
	InMemoryTripleContainer triples = (InMemoryTripleContainer) context
		.getObjectFromCache("intermediateTuples");
	InMemoryTripleContainer explicitTriples = (InMemoryTripleContainer) context
		.getObjectFromCache("explicitIntermediateTuples");

	// Retrieve current inferred and explicit triples
	List<Object[]> remoteObjs = context.retrieveRemoteCacheObjects(
		"intermediateTuples", "explicitIntermediateTuples");
	if (remoteObjs != null) {
	    for (Object[] values : remoteObjs) {
		// Inferred
		InMemoryTripleContainer v = (InMemoryTripleContainer) values[0];
		if (v != null && v.size() > 0) {
		    if (triples == null) {
			triples = v;
		    } else {
			triples.addAll(v);
		    }
		}

		// Explicit
		v = (InMemoryTripleContainer) values[1];
		if (v != null && v.size() > 0) {
		    if (explicitTriples == null) {
			explicitTriples = v;
		    } else {
			explicitTriples.addAll(v);
		    }
		}
	    }
	}

	// Remove eventual inferred triples that are explicit in DB
	if (explicitTriples != null && triples != null) {
	    triples.removeAll(explicitTriples);
	}

	if (triples != null)
	    context.incrCounter("total results", triples.size());	

	// Check whether I need to relaunch the query
	if (triples != null && triples.size() > 0) {
	    InMemoryTripleContainer previousTriples = (InMemoryTripleContainer) context
		    .getObjectFromCache("inputIntermediateTuples");
	    if (previousTriples != null) {
		if (explicitTriples != null)
		    previousTriples.addAll(explicitTriples);
		previousTriples.addAll(triples);
		previousTriples.index();
	    } else {
		if (explicitTriples != null)
		    triples.addAll(explicitTriples);
		context.putObjectInCache("inputIntermediateTuples", triples);
		triples.index();
	    }
	    context.putObjectInCache("intermediateTuples", null);

	    context.broadcastCacheObjects("inputIntermediateTuples",
		    "intermediateTuples");

	    /***** LAUNCH A NEW CHAIN *****/
	    Chain newChain = new Chain();
	    chain.generateChild(context, newChain);

	    // Original query
	    triple[0].setValue((Long) params[0]);
	    triple[1].setValue((Long) params[1]);
	    triple[2].setValue((Long) params[2]);
	    Tuple tuple = new Tuple();
	    tuple.set(triple);

	    RuleBCAlgo.applyTo(context, tuple, newChain);

	    newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);
	    newChain.setInputTuple(tuple);
	    newChains.add(newChain);
	} else {
	    if (explicitTriples != null)
		    context.incrCounter("total results", explicitTriples.size());
	}
	params = null;
	outputContainer = null;
    }

    public static void cleanup(ActionContext context) {
	context.putObjectInCache("intermediateTuples", null);
	context.putObjectInCache("inputIntermediateTuples", null);
	context.putObjectInCache("outputSoFar", null);
	context.putObjectInCache("explicitIntermediateTuples", null);
	context.broadcastCacheObjects("intermediateTuples",
		"inputIntermediateTuples", "explicitIntermediateTuples");
    }
}
