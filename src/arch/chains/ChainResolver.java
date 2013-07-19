package arch.chains;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.StatisticsCollector;
import arch.actions.Action;
import arch.actions.ActionsProvider;
import arch.data.types.DataProvider;
import arch.data.types.Tuple;
import arch.net.NetworkLayer;
import arch.storage.Container;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ChainResolver implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainResolver.class);

	Container<Chain> chainsToResolve;
	NetworkLayer net;
	StatisticsCollector stats;
	Context context;

	ActionsProvider ap;
	DataProvider dp;

	public ChainResolver(Context context) {
		try {
			this.stats = context.getStatisticsCollector();
			this.chainsToResolve = context.getChainsToResolve();
			this.net = context.getNetworkLayer();
			this.context = context;
			this.ap = context.getActionsProvider();
			this.dp = context.getDataProvider();
		} catch (Exception e) {
			log.error("Failed init", e);
		}
	}

	@Override
	public void run() {
		try {
			// Init action context
			ActionContext ac = new ActionContext(context, dp);
			long chainIDCounter = (context.getNetworkLayer().getCounter(
					"chainID") + 1) << 40;
			ac.setStartingChainID(chainIDCounter);
			int bucketIDCounter = ((int) context.getNetworkLayer().getCounter(
					"bucketID") + 1) << 16;
			ac.setStartingBucketID(bucketIDCounter);

			WritableContainer<Chain> chainsToProcess = new WritableContainer<Chain>(
					Consts.SIZE_BUFFERS_CHILDREN_CHAIN_RESOLVE);
			WritableContainer<Chain> chainsToSend = new WritableContainer<Chain>(
					Consts.SIZE_BUFFERS_CHILDREN_CHAIN_RESOLVE);
			Chain chain = new Chain();
			Tuple tuple = new Tuple();

			while (true) {
				chainsToResolve.remove(chain);

				ac.setCurrentChain(chain);
				// Get available actions
				String[] actions = chain.getAvailableActions();

				boolean excludeFlag = chain.getExcludeExecution();
				chain.setExcludeExecution(false);
				chain.getInputTuple(tuple);
				chainsToProcess.clear();
				chainsToSend.clear();

				// Expand the chain
				if (actions != null) {
					for (String sAction : actions) {
						Action rule = ap.get(sAction);
						rule.apply(ac, tuple, chain, chainsToProcess,
								chainsToSend);
						ap.release(rule);
					}
				}

				chain.setExcludeExecution(excludeFlag);

				if (chainsToProcess.getNElements() > 0) {
					stats.addCounter(chain.getSubmissionNode(),
							chain.getSubmissionId(),
							"Chains Generated From Expansion",
							chainsToProcess.getNElements());
					chainsToResolve.addAll(chainsToProcess);
				}

				if (chainsToSend.getNElements() > 0) {
					stats.addCounter(chain.getSubmissionNode(),
							chain.getSubmissionId(),
							"Chains Sent From Expansion",
							chainsToSend.getNElements());
					net.sendChains(chainsToSend);
				}
				net.sendChain(chain);
			}
		} catch (Exception e) {
			log.error("Error in the main execution of the resolver thread", e);
		}
	}
}
