package arch;

import arch.actions.ActionsProvider;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.chains.ChainNotifier;
import arch.data.types.DataProvider;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.InputLayerRegistry;
import arch.net.NetworkLayer;
import arch.storage.Container;
import arch.storage.Factory;
import arch.storage.SubmissionCache;
import arch.submissions.SubmissionRegistry;
import arch.utils.Configuration;

public class Context {

	private InputLayerRegistry input;
	private Configuration conf;
	private Buckets container;
	private SubmissionRegistry registry;
	private Container<Chain> chainsToResolve;
	private Container<Chain> chainsToProcess;
	private NetworkLayer net;
	private StatisticsCollector stats;
	private ActionsProvider actionProvider;
	private DataProvider dataProvider;
	private Factory<Tuple> defaultTupleFactory;
	private SubmissionCache cache;
	private ChainNotifier chainNotifier;

	public void init(InputLayerRegistry input, Buckets container,
			SubmissionRegistry registry, Container<Chain> chainsToResolve,
			Container<Chain> chainsToProcess, ChainNotifier notifier, NetworkLayer net,
			StatisticsCollector stats, ActionsProvider actionProvider,
			DataProvider dataProvider, Factory<Tuple> defaultTupleFactory,
			SubmissionCache cache, Configuration conf) {
		this.input = input;
		this.conf = conf;
		this.container = container;
		this.registry = registry;
		this.chainsToResolve = chainsToResolve;
		this.chainsToProcess = chainsToProcess;
		this.chainNotifier = notifier;
		this.net = net;
		this.stats = stats;
		this.actionProvider = actionProvider;
		this.dataProvider = dataProvider;
		this.defaultTupleFactory = defaultTupleFactory;
		this.cache = cache;
	}
	
	public SubmissionCache getSubmissionCache() {
		return cache;
	}
	
	public Factory<Tuple> getDeFaultTupleFactory() {
		return defaultTupleFactory;
	}

	public StatisticsCollector getStatisticsCollector() {
		return stats;
	}

	public NetworkLayer getNetworkLayer() {
		return net;
	}

	public InputLayer getInputLayer(int idInputLayer) {
		return input.getInputLayer(idInputLayer);
	}

	public Configuration getConfiguration() {
		return conf;
	}

	public Buckets getTuplesBuckets() {
		return container;
	}

	public SubmissionRegistry getSubmissionsRegistry() {
		return registry;
	}

	public Container<Chain> getChainsToResolve() {
		return chainsToResolve;
	}

	public Container<Chain> getChainsToProcess() {
		return chainsToProcess;
	}
	
	public ChainNotifier getChainNotifier() {
	    return chainNotifier;
	}

	public ActionsProvider getActionsProvider() {
		return actionProvider;
	}

	public DataProvider getDataProvider() {
		return dataProvider;
	}
}