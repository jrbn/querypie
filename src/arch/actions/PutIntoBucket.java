package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.buckets.Bucket;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class PutIntoBucket extends Action {

    static final Logger log = LoggerFactory.getLogger(PutIntoBucket.class);

    Bucket bucket = null;

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
	    WritableContainer<Chain> outputChains, WritableContainer<Chain> chainsToSend) throws Exception {
	return applyTo(context, tuple, inputChain, outputChains);
    }

    public static Chain applyTo(ActionContext context, Tuple tuple,
	    Chain inputChain, WritableContainer<Chain> outputChains)
	    throws Exception {
	TInt bucketID = new TInt();
	tuple.get(bucketID, 0);
	inputChain.addAction(PutIntoBucket.class.getName(), tuple,
		bucketID.getValue());
	return inputChain;
    }

    int bucketID;

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) {
	int id = (Integer) params[0];
	bucket = context.getTuplesBuckets().getOrCreateBucket(
		chain.getSubmissionNode(), chain.getSubmissionId(), id, null,
		null);
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    // Action[] actionsInChain, int indexAction,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	bucket.add(inputTuple);
	output.add(inputTuple);
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
	    throws Exception {
	bucket.setFinished(true);
    }
}
