package nl.vu.cs.querypie.reasoner;

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

public class RuleGetPattern extends Action {

    public static Chain applyTo(ActionContext context, Tuple tuple, Chain chain)
	    throws Exception {

	chain.addAction(RuleGetPattern.class.getName(), tuple, (Object[]) null);

	TBoolean converge = new TBoolean(false);
	if (tuple.getNElements() > 0)
	    tuple.get(converge, 0);

	if (converge.getValue()) {
	    FilterDuplicates.applyTo(context, null, chain, null);

	    SendTo.applyTo(context, new Tuple(new TString(SendTo.THIS),
		    new TBoolean(true), new TInt(context.getNewBucketID()),
		    new TString(TupleComparator.class.getName())), chain, null);
	}

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
