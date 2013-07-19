package nl.vu.cs.querypie.sparql;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class RemoveLastElement extends Action {

    public static Chain applyTo(ActionContext context, Tuple tuple, Chain chain)
	    throws Exception {
	chain.addAction(RemoveLastElement.class.getName(), null,
		(Object[]) null);
	return chain;
    }

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	return applyTo(context, tuple, chain);
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	if (inputTuple.getNElements() == 4)
	    inputTuple.removeLast();
	output.add(inputTuple);
    }

}
