package nl.vu.cs.querypie.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.TLong;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class SPARQLCheckBindings extends Action {

    static final Logger log = LoggerFactory
	    .getLogger(SPARQLCheckBindings.class);

    long[] t = new long[3];
    long chainId, responsibleChain;
    int replicatedFactor, nodeId, position, bucket;

    public static Chain applyTo(ActionContext context, Tuple tuple,
	    Chain chain, WritableContainer<Chain> childrenChains)
	    throws Exception {
	TInt bucket = new TInt();
	TLong s = new TLong();
	TLong p = new TLong();
	TLong o = new TLong();
	TInt pos = new TInt();
	tuple.get(bucket, s, p, o, pos);

	chain.addAction(SPARQLCheckBindings.class.getName(), null,
		chain.getChainId(), bucket.getValue(), s.getValue(), p
			.getValue(), o.getValue(), pos.getValue(), context
			.getNetworkLayer().getMyPartition());

	return chain;
    }

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	return SPARQLCheckBindings.applyTo(context, tuple, chain,
		chainsToResolve);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {
	replicatedFactor = chain.getReplicatedFactor();
	chainId = chain.getChainId();
	responsibleChain = (Long) params[0];
	bucket = (Integer) params[1];
	t[0] = (Long) params[2];
	t[1] = (Long) params[3];
	t[2] = (Long) params[4];
	position = (Integer) params[5];
	nodeId = (Integer) params[6];
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	output.add(inputTuple);
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output,
	    WritableContainer<Chain> newChains,
	    WritableContainer<Chain> chainsToSend) throws Exception {

	if (chainId == responsibleChain && replicatedFactor > 0) {
	    Chain newChain = new Chain();
	    chain.generateChild(context, newChain);
	    
	    Tuple tuple = new Tuple();
	    tuple.set(new TLong(t[0]), new TLong(t[1]), new TLong(t[2]),
		    new TInt(position));
	    SPARQLExecuteDeeperRules.applyTo(context, tuple, newChain, null);

	    /***** READ THE TUPLES FROM THE BUCKET *****/
	    newChain.setExcludeExecution(false);
	    newChain.setChainChildren(0);
	    newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
	    tuple.set(new TInt(nodeId),
		    new TInt(newChain.getSubmissionId()), new TInt(bucket),
		    new TInt(chain.getCustomFlag()));
	    newChain.setInputTuple(tuple);

	    newChains.add(newChain);
	}
    }
}
