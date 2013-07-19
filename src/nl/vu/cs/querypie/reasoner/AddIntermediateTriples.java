package nl.vu.cs.querypie.reasoner;

import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class AddIntermediateTriples extends Action {

    static final Logger log = LoggerFactory
	    .getLogger(AddIntermediateTriples.class);

    RDFTerm a = new RDFTerm(), b = new RDFTerm(), c = new RDFTerm();
    InMemoryTripleContainer set = new InMemoryTripleContainer();
    InMemoryTripleContainer input = null;
    // Object[] params;
    // int count;

    InMemoryTripleContainer alreadyExplicit = null;
    InMemoryTripleContainer explicitSet = new InMemoryTripleContainer();

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	RDFTerm[] t = { new RDFTerm(), new RDFTerm(), new RDFTerm() };
	tuple.get(t);
	return applyTo(context, t[0].getValue(), t[1].getValue(),
		t[2].getValue(), chain);
    }

    public static Chain applyTo(ActionContext context, long t1, long t2,
	    long t3, Chain chain) throws Exception {
	chain.addAction(AddIntermediateTriples.class.getName(), null, t1, t2,
		t3);
	return chain;
    }

    long[] query = new long[3];

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {
	// count = 0;
	input = (InMemoryTripleContainer) context
		.getObjectFromCache("inputIntermediateTuples");
	set.clear();

	alreadyExplicit = (InMemoryTripleContainer) context
		.getObjectFromCache("explicitIntermediateTuples");
	explicitSet.clear();

	query[0] = (Long) params[0];
	query[1] = (Long) params[1];
	query[2] = (Long) params[2];
	set.addQuery((Long) params[0], (Long) params[1], (Long) params[2],
		context, input);
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {

	inputTuple.get(a, b, c);
	if (inputTuple.getNElements() == 4) {
	    if (set.addTriple(a, b, c, input)) {
		// count++;
		output.add(inputTuple);
	    }
	} else { // Explicit tuple
	    explicitSet.addTriple(a, b, c, alreadyExplicit, input);
	    output.add(inputTuple);
	}
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output,
	    WritableContainer<Chain> newChains,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	// context.incrCounter("intermediate tuples", count);

	// log.debug("This action" + Arrays.toString(query) + " has added " +
	// count + " new triples");

	if (set.isToCopy()) {
	    InMemoryTripleContainer current = (InMemoryTripleContainer) context
		    .getObjectFromCache("intermediateTuples");
	    if (current == null) {
		synchronized (InMemoryTripleContainer.class) {
		    current = (InMemoryTripleContainer) context
			    .getObjectFromCache("intermediateTuples");
		    if (current == null) {
			current = set;
			set = new InMemoryTripleContainer();
			context.putObjectInCache("intermediateTuples", current);
		    } else {
			current.addAll(set);
		    }
		}
	    } else {
		current.addAll(set);
	    }
	}

	if (explicitSet.size() > 0) {
	    if (alreadyExplicit == null) {
		synchronized (InMemoryTripleContainer.class) {
		    alreadyExplicit = (InMemoryTripleContainer) context
			    .getObjectFromCache("explicitIntermediateTuples");
		    if (alreadyExplicit == null) {
			alreadyExplicit = explicitSet;
			explicitSet = new InMemoryTripleContainer();
			context.putObjectInCache("explicitIntermediateTuples",
				alreadyExplicit);
		    } else {
			alreadyExplicit.addAll(explicitSet);
			explicitSet.clear();
		    }
		}
	    } else {
		alreadyExplicit.addAll(explicitSet);
		explicitSet.clear();
	    }
	}

	// Clear what needs to be cleared, this object will be cached. --Ceriel
	set.clear();
	alreadyExplicit = null;
	input = null;
    }
}
