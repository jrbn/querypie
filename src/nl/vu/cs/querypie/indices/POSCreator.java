package nl.vu.cs.querypie.indices;

import nl.vu.cs.querypie.storage.RDFTerm;
import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class POSCreator extends Action {

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain chain,
			WritableContainer<Chain> chainsToResolve, WritableContainer<Chain> chainsToSend) throws Exception {
		chain.addAction(this, null, (Object[]) null);
		return chain;
	}

	RDFTerm s = new RDFTerm();
	RDFTerm p = new RDFTerm();
	RDFTerm o = new RDFTerm();
	Tuple tuple = new Tuple();

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			// Action[] actionsInChain, int indexAction,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		inputTuple.get(s, p, o);
		tuple.set(p, o, s);
		output.add(tuple);
	}

}
