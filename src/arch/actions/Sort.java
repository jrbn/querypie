package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Utils;

public class Sort extends Action {

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain chain,
			WritableContainer<Chain> chainsToResolve, WritableContainer<Chain> chainsToSend) throws Exception {
		Object[] actionParams = null;
		TString sortingFunction = new TString();

		if (tuple.getType(0) == sortingFunction.getIdDatatype()) {
			actionParams = new Object[1 + tuple.getNElements()];

			actionParams[0] = 1; // One stream
			tuple.get(sortingFunction);
			actionParams[1] = sortingFunction.getValue();
			Utils.readObjectsFromTuple(tuple, 1, context.getDataProvider(),
					actionParams, 2, tuple.getNElements() - 2);
		} else {
			actionParams = new Object[tuple.getNElements()];
			TInt m = new TInt();
			tuple.get(m);
			actionParams[0] = m.getValue();
			tuple.get(sortingFunction);
			actionParams[1] = sortingFunction.getValue();
			Utils.readObjectsFromTuple(tuple, 2, context.getDataProvider(),
					actionParams, 2, tuple.getNElements() - 3);
		}

		chain.addAction(this, null, actionParams);
		return chain;
	}

	@Override
	public void startProcess(ActionContext context, Chain chain,
			Object... params) throws Exception {
		// TODO: fill parameters
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			// Action[] actionsInChain, int indexAction,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		// TODO Put it into streams
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
			throws Exception {
		// TODO sends the streams, buckets tuples
	}
}
