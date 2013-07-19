package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class FilterDuplicates extends Action {

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
	    throws Exception {
	process(null, chain, null, null, output, context);
    }

    private Tuple tuple = new Tuple();
    boolean first = true;
    long filtered = 0;
    long total = 0;

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {
	first = true;
	filtered = total = 0;
    }

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	return applyTo(context, tuple, chain, chainsToResolve);
    }

    public static Chain applyTo(ActionContext context, Tuple tuple,
	    Chain chain, WritableContainer<Chain> childrenChains)
	    throws Exception {
	chain.addAction(FilterDuplicates.class.getName(), null, (Object[]) null);
	return chain;
    }

    @Override
    public void process(Tuple inputTuple,
	    Chain remainingChain,
	    // Action[] actionsInChain, int indexAction,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {

	if (inputTuple == null) {
	    if (!first) {
		output.add(tuple);
		// System.out.println("Output tuple=" + tuple.toString(new
		// DataProvider()));
	    }
	} else {
	    if (first) {
		inputTuple.copyTo(tuple);
		first = false;
	    } else {
		if (tuple.compareTo(inputTuple) != 0) {
		    output.add(tuple);
		    // System.out.println("Output tuple=" + tuple.toString(new
		    // DataProvider()));
		    inputTuple.copyTo(tuple);
		}
	    }
	}
    }

}
