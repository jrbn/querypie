package arch.actions;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.buckets.Bucket;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.data.types.DataProvider;
import arch.data.types.SimpleData;
import arch.data.types.TBoolean;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.net.NetworkLayer;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;
import arch.utils.Utils;

public class SendTo extends Action {

	public static final String THIS = "this";
	public static final String MULTIPLE = "multiple";
	public static final String ALL = "all";
	public static final String RANDOM = "random";

	static final Logger log = LoggerFactory.getLogger(SendTo.class);

	int submissionNode;
	int idSubmission;
	long chainId;
	long parentChainId;
	int nchildren;
	int replicatedFactor;
	long responsibleChain;

	int myNodeId;
	int nodeId;
	int bucket;
	boolean sc;
	boolean ft;
	String sortingFunction;

	Object[] sortingParams;
	TInt tbucket = new TInt();
	TString tsorting = new TString();
	TInt tsub = new TInt();
	TInt tnode = new TInt();

	Tuple tuple = new Tuple();

	NetworkLayer net = null;
	Bucket[] bucketsCache;
	Buckets buckets = null;

	@Override
	public boolean blockProcessing() {
		return sc;
	}

	public long getResponsibleChainID() {
		return responsibleChain;
	}

	public int getBucketId() {
		return bucket;
	}

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
			WritableContainer<Chain> outputChains,
			WritableContainer<Chain> chainsToSend) throws Exception {
		return SendTo.applyTo(context, tuple, inputChain, outputChains);
	}

	static Random r = new Random();

	public static Chain applyTo(ActionContext context, Tuple tuple,
			Chain inputChain, WritableContainer<Chain> outputChains)
			throws Exception {

		TString snode = new TString();
		TBoolean sendChain = new TBoolean();
		TInt tbucket = new TInt();
		TString tsorting = new TString();
		TBoolean forwardTuples = new TBoolean();
		Object[] sortingParams = null;

		int bucketID;
		String sorting = "";
		int lengthSortingParams = 0;

		boolean forward = false;
		if (tuple.getNElements() == 2) {
			tuple.get(snode, sendChain);
			bucketID = context.getNewBucketID();
			forward = !sendChain.getValue();
		} else if (tuple.getNElements() == 3) {
			if (tuple.getType(2) == tbucket.getIdDatatype()) {
				tuple.get(snode, sendChain, tbucket);
				bucketID = tbucket.getValue();
				forward = !sendChain.getValue();
			} else {
				tuple.get(snode, sendChain, tsorting);
				bucketID = context.getNewBucketID();
				sorting = tsorting.getValue();
				forward = !sendChain.getValue();
			}
		} else if (tuple.getNElements() == 4) { // length = 4
			if (tuple.getType(3) == tsorting.getIdDatatype()) {
				tuple.get(snode, sendChain, tbucket, tsorting);
				bucketID = tbucket.getValue();
				sorting = tsorting.getValue();
				forward = !sendChain.getValue();
			} else {
				tuple.get(snode, sendChain, tbucket, forwardTuples);
				bucketID = tbucket.getValue();
				forward = forwardTuples.getValue();
			}
		} else { // Variable length
			tuple.get(snode, sendChain, tbucket, forwardTuples, tsorting);
			bucketID = tbucket.getValue();
			sorting = tsorting.getValue();
			forward = forwardTuples.getValue();

			lengthSortingParams = tuple.getNElements() - 5;
			if (lengthSortingParams > 0) {
				sortingParams = new Object[lengthSortingParams];
				Utils.readObjectsFromTuple(tuple, 5, context.getDataProvider(),
						sortingParams);
			}
		}

		int idNode = 0;
		if (snode.getValue().equals(THIS)) {
			idNode = context.getNetworkLayer().getMyPartition();
		} else if (snode.getValue().equals(RANDOM)) {
			idNode = r.nextInt(context.getNetworkLayer().getNumberNodes());
		} else if (snode.getValue().equals(MULTIPLE)) {
			idNode = -1; // Partition the output
		} else if (snode.getValue().equals(ALL)) {
			idNode = -2; // All nodes
		} else {
			// Try to parse the number
			try {
				idNode = Integer.valueOf(snode.getValue());
			} catch (Exception e) {
				log.warn("Unrecognized node (" + snode + ")! Set node=0");
				idNode = 0;
			}
		}

		if (sorting == null) {
			sorting = "";
		}

		Object[] actionParams = new Object[7 + lengthSortingParams];
		actionParams[0] = idNode;
		actionParams[1] = bucketID;
		actionParams[2] = inputChain.getChainId();
		actionParams[3] = sendChain.getValue();
		actionParams[4] = forward;
		actionParams[5] = sorting;
		actionParams[6] = lengthSortingParams;
		for (int i = 0; i < lengthSortingParams; ++i) {
			actionParams[7 + i] = sortingParams[i];
		}

		inputChain.addAction(SendTo.class.getName(), tuple, actionParams);
		return inputChain;
	}

	@Override
	public void startProcess(ActionContext context, Chain chain,
			Object... params) {
		// Read params
		nodeId = (Integer) params[0];
		bucket = (Integer) params[1];
		responsibleChain = (Long) params[2];
		sc = (Boolean) params[3];
		ft = (Boolean) params[4];
		sortingFunction = (String) params[5];
		int lengthSortingParams = (Integer) params[6];
		if (lengthSortingParams > 0) {
			sortingParams = new Object[lengthSortingParams];
			for (int i = 0; i < lengthSortingParams; ++i) {
				sortingParams[i] = params[i + 7];
			}
		} else {
			sortingParams = null;
		}
		if (sortingFunction == null || sortingFunction.equals("")) {
			sortingFunction = null;
			sortingParams = null;
		}

		// Init variables
		net = context.getNetworkLayer();
		bucketsCache = new Bucket[net.getNumberNodes()];
		submissionNode = chain.getSubmissionNode();
		idSubmission = chain.getSubmissionId();
		chainId = chain.getChainId();
		parentChainId = chain.getParentChainId();
		replicatedFactor = chain.getReplicatedFactor();

		myNodeId = net.getMyPartition();
		buckets = context.getTuplesBuckets();
		if (log.isDebugEnabled()) {
			log.debug("SendTo.startProcess, SendTo = " + this + ": bucket = "
					+ Buckets.getKey(idSubmission, bucket));
		}
	}

	@Override
	public void process(Tuple tuple,
			Chain remainingChain,
			// Action[] actionsInChain, int indexAction,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> outputTuples, ActionContext context) {
		try {

			int nodeId = this.nodeId;
			if (nodeId == -1) {
				// Node to send is tuple's last record. Must remove it
				tuple.get(tnode, tuple.getNElements() - 1);
				nodeId = tnode.getValue();
				tuple.removeLast();
			}

			if (ft) {
				outputTuples.add(tuple);
			}

			if (nodeId >= 0) {
				Bucket b = bucketsCache[nodeId];
				if (b == null) {
					// Copy the tuple to the local buffer
					// b = buckets.getOrCreateBucket(submissionNode,
					// idSubmission, sortingFunction, sortingParams);
					b = buckets.startTransfer(submissionNode, idSubmission,
							nodeId, bucket, sortingFunction, sortingParams);
					bucketsCache[nodeId] = b;
				}
				b.add(tuple);
			} else { // Send it to all the nodes

				for (int i = 0; i < net.getNumberNodes(); ++i) {
					Bucket b = bucketsCache[i];
					if (b == null) {
						// Copy the tuple to the local buffer
						b = buckets.startTransfer(submissionNode, idSubmission,
								i, bucket, sortingFunction, sortingParams);
						bucketsCache[i] = b;
					}
					b.add(tuple);
				}

			}

		} catch (Exception e) {
			log.error(
					"Failed processing tuple. Chain="
							+ remainingChain.toString(new ActionsProvider(),
									new DataProvider(), -1), e);
		}
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> outputTuples,
			WritableContainer<Chain> newChains,
			WritableContainer<Chain> chainsToSend) {
		try {
			Chain newChain = new Chain();
			// Send the chains to process the buckets to all the nodes that
			// will host the buckets
			if (sc && chainId == responsibleChain && replicatedFactor > 0) {
				/*** AT FIRST SEND THE CHAINS ***/
				chain.copyTo(newChain);
				newChain.setExcludeExecution(false);
				newChain.setChainChildren(0);
				newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
				tsub.setValue(idSubmission);
				tnode.setValue(nodeId);
				tbucket.setValue(bucket);
				if (sortingFunction != null) {
					tsorting.setValue(sortingFunction);
					if (sortingParams != null && sortingParams.length > 0) {
						SimpleData[] params = new SimpleData[4 + sortingParams.length];
						params[0] = tsub;
						params[1] = tbucket;
						params[2] = tsorting;
						Utils.writeObjectsOnSimpleDataArray(params, 3,
								sortingParams, context.getDataProvider());
						params[params.length - 1] = tnode;
						this.tuple.set(params);
						for (int i = 0; i < sortingParams.length; ++i) {
							context.getDataProvider().release(params[3 + i]);
						}

						this.tuple.set(params);
					} else {
						this.tuple.set(tsub, tbucket, tsorting, tnode);
					}
				} else {
					this.tuple.set(tsub, tbucket, tnode);
				}
				newChain.setInputTuple(this.tuple);
				context.getNetworkLayer().sendChain(newChain);
			}

			nchildren = chain.getChainChildren();

			int startNode, endNode;
			if (nodeId == -1 || nodeId == -2) {
				startNode = 0;
				endNode = net.getNumberNodes();
			} else if (nodeId == -3) {
				startNode = myNodeId;
				endNode = myNodeId + 1;
			} else {
				startNode = nodeId;
				endNode = nodeId + 1;
			}

			if (log.isDebugEnabled()) {
				log.debug("SendTo.stopProcess: SendTo = " + this
						+ ", startNode = " + startNode + ", endNode = "
						+ endNode + ", bucketID = "
						+ Buckets.getKey(idSubmission, bucket) + ", chainID = "
						+ chainId);
			}

			while (startNode < endNode) {
				buckets.finishTransfer(submissionNode, idSubmission, startNode,
						this.bucket, this.chainId, this.parentChainId,
						this.nchildren, this.replicatedFactor,
						this.responsibleChain == this.chainId, sortingFunction,
						sortingParams, bucketsCache[startNode] != null);
				++startNode;
			}
		} catch (Exception e) {
			log.error("Error", e);
		}
		bucketsCache = null;
		net = null;
		buckets = null;
	}
}
