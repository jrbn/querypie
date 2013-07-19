package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

abstract public class Partitioner extends Action {

    static final Logger log = LoggerFactory.getLogger(Partitioner.class);

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
	    WritableContainer<Chain> outputChains, WritableContainer<Chain> chainsToSend) throws Exception {
	if (tuple != null && tuple.getNElements() == 2) {
	    TInt nnodes = new TInt();
	    tuple.get(nnodes, 0);
	    inputChain.addAction(this, null, nnodes.getValue());
	} else {
	    inputChain.addAction(this, null, context.getNetworkLayer()
		    .getNumberNodes());
	}

	return inputChain;
    }

    Tuple tuple = new Tuple();
    TInt tnode = new TInt();
    int nnodes;

    protected abstract int partition(Tuple tuple, int nnodes) throws Exception;

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) {
	nnodes = (Integer) params[0];
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    // Action[] actionsInChain, int indexAction,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	inputTuple.copyTo(tuple);

	tnode.setValue(partition(inputTuple, nnodes));
	tuple.add(tnode);
	output.add(tuple);
    }
}
