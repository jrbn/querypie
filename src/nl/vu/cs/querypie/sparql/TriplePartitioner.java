package nl.vu.cs.querypie.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Partitioner;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class TriplePartitioner extends Partitioner {
    Logger log = LoggerFactory.getLogger(TriplePartitioner.class);

    public static Chain applyTo(ActionContext context, Tuple tuple,
	    Chain inputChain, WritableContainer<Chain> outputChains)
	    throws Exception {
	inputChain.addAction(TriplePartitioner.class.getName(), null, context
		.getNetworkLayer().getNumberNodes());
	return inputChain;
    }

    @Override
    protected int partition(Tuple tuple, int nnodes) {
	try {
	    return Math.abs(tuple.getHash(24) % nnodes);
	} catch (Exception e) {
	    log.error("Error in partitioning", e);
	}

	return 0;
    }

}
