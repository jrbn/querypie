package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.SimpleData;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class Project extends Action {

	public static Chain applyTo(ActionContext context, Tuple tuple,
			Chain inputChain, WritableContainer<Chain> outputChains)
			throws Exception {
		Object[] params = new Object[tuple.getNElements() + 1];
		TInt value = new TInt();
		params[0] = tuple.getNElements();
		for (int i = 0; i < tuple.getNElements(); ++i) {
			tuple.get(value, i);
			params[i + 1] = value.getValue();
		}
		inputChain.addAction(Project.class.getName(), null, params);
		return inputChain;
	}

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain chain,
			WritableContainer<Chain> chainsToResolve, WritableContainer<Chain> chainsToSend) throws Exception {
		return applyTo(context, tuple, chain, chainsToResolve);
	}

	Tuple tuple = new Tuple();
	SimpleData[] outputFields;
	int[] positions;
	boolean first;

	@Override
	public void startProcess(ActionContext context, Chain chain,
			Object... params) throws Exception {
		first = true;
		positions = new int[(Integer) params[0]];
		for (int i = 0; i < positions.length; ++i) {
			positions[i] = (Integer) params[i + 1];
		}
		outputFields = new SimpleData[positions.length];
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
			throws Exception {
		for (int i = 0; i < outputFields.length; ++i) {
			if (outputFields[i] != null)
				context.getDataProvider().release(outputFields[i]);
		}
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			// Action[] actionsInChain, int indexAction,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		if (first) {
			first = false;
			// Populate the output fields
			for (int i = 0; i < positions.length; ++i) {
				SimpleData data = context.getDataProvider().get(
						inputTuple.getType(positions[i]));
				outputFields[i] = data;
			}
		}

		for (int i = 0; i < outputFields.length; ++i) {
			inputTuple.get(outputFields[i], i);
		}

		tuple.set(outputFields);
		output.add(tuple);
	}

}
