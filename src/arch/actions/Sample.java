package arch.actions;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class Sample extends Action {

	static final Logger log = LoggerFactory.getLogger(Sample.class);

	Tuple tuple = new Tuple();

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
			WritableContainer<Chain> outputChains,
			WritableContainer<Chain> chainsToSend) throws Exception {
		TInt sample = new TInt();
		tuple.get(sample);
		inputChain.addAction(this, null, sample.getValue());

		return inputChain;
	}

	int param1;
	Random rand = new Random();

	@Override
	public void startProcess(ActionContext context, Chain chain,
			Object... params) {
		param1 = (Integer) params[0];
	}

	@Override
	public void process(Tuple inputTuple,
			Chain remainingChain,
			// Action[] actionsInChain, int indexAction,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		if (tuple != null) {
			if (rand.nextInt(100) < param1) {
				output.add(inputTuple);
			}
		}
	}
}
