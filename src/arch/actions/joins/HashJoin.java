package arch.actions.joins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.buckets.BucketIterator;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class HashJoin extends Action {

	static final Logger log = LoggerFactory.getLogger(HashJoin.class);

	public static Chain applyTo(ActionContext context, Tuple tuple,
			Chain inputChain, WritableContainer<Chain> childrenChains)
			throws Exception {
		TInt bucketID = new TInt();
		TInt lengthJoin = new TInt();
		tuple.get(bucketID, lengthJoin);

		Object[] params = new Object[3 + lengthJoin.getValue() * 2];
		params[0] = inputChain.getChainId();
		params[1] = bucketID.getValue();
		params[2] = lengthJoin.getValue();

		TInt index = new TInt();
		for (int i = 0; i < lengthJoin.getValue(); ++i) {
			tuple.get(index, 2 + i);
			params[3 + i] = index.getValue();
		}

		for (int i = 0; i < lengthJoin.getValue(); ++i) {
			tuple.get(index, 2 + lengthJoin.getValue() + i);
			params[3 + lengthJoin.getValue() + i] = index.getValue();
		}

		inputChain.addAction(HashJoin.class.getName(), null, params);
		return inputChain;
	}

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToSend) throws Exception {
		return applyTo(context, tuple, inputChain, chainsToResolve);
	}

	int lengthJoins = 0;
	int[] bucketIndexJoins = null;
	int[] inputIndexJoins = null;
	int lengthConcatFields = 0;
	int[] concatFields = new int[20];
	boolean first = true;

	Tuple tupleBucket = new Tuple();
	Map<Tuple, List<Tuple>> map = null;

	long responsibleChain = 0;
	int bucketID = 0;

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context, Chain chain,
			Object... params) throws Exception {
		// log.debug("Start HashJoin ...");

		responsibleChain = (Long) params[0];
		bucketID = (Integer) params[1];

		// Set the indexes of the fields to be joined
		lengthJoins = (Integer) params[2];
		bucketIndexJoins = new int[lengthJoins];
		inputIndexJoins = new int[lengthJoins];
		for (int i = 0; i < lengthJoins; ++i) {
			bucketIndexJoins[i] = (Integer) params[3 + i];
			inputIndexJoins[i] = (Integer) params[3 + lengthJoins + i];
		}

		// Init the bucket
		map = null;
		if (chain.getChainId() == responsibleChain) {
			if (log.isDebugEnabled()) {
				log.debug("Fetch the content from the bucket " + bucketID);
			}
			// Factory<Tuple> tf = context.getDeFaultTupleFactory();
			map = new HashMap<Tuple, List<Tuple>>();
			TupleIterator itr = context.getTuplesBuckets().getIterator(
					chain.getSubmissionId(), bucketID);
			int count = 0;
			// Back storage for tuples in the HashMap.
			byte[] buf = new byte[1024 * 100];
			int offset = 0;
			while (itr.next()) {
				count++;
				if (buf.length - offset < Consts.MAX_TUPLE_SIZE) {
					// Not enough room. Allocate new space.
					buf = new byte[1024 * 100];
					offset = 0;
				}
				Tuple key = new Tuple(buf, offset);
				itr.getTuple(key);
				offset = key.getEnd(); // this is where the next tuple will
				// start.
				key.setHashCodeFields(bucketIndexJoins);
				List<Tuple> values = map.get(key);
				if (values == null) {
					values = new ArrayList<Tuple>();
					map.put(key, values);
				}
				values.add(key);
			}
			context.getTuplesBuckets().releaseIterator((BucketIterator) itr,
					true);

			if (log.isDebugEnabled()) {
				log.debug("Size of the HashMap: " + count);
			}
			context.putObjectInCache(bucketID, map);
		} else {
			// Get it from the cache
			// log.debug("Fetch the content from the cache");
			map = (Map<Tuple, List<Tuple>>) context
					.getObjectFromCache(bucketID);
			while (map == null) {
				Thread.sleep(10); // Sleep 10ms before trying again
				map = (Map<Tuple, List<Tuple>>) context
						.getObjectFromCache(bucketID);
			}
			// log.debug("Got the HashMap from cache");
		}

		first = true;
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			// Action[] actionsInChain, int indexAction,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		if (first) {
			first = false;
			// Calculates the joins of the bucket side that need to be
			// concatenated
			lengthConcatFields = 0;
			for (int i = 0; i < inputTuple.getNElements(); ++i) {
				boolean found = false;
				for (int j = 0; j < lengthJoins; ++j) {
					if (inputIndexJoins[j] == i) {
						found = true;
						break;
					}
				}
				if (!found) {
					concatFields[lengthConcatFields++] = i;
				}
			}
		}

		inputTuple.setHashCodeFields(inputIndexJoins);
		List<Tuple> values = map.get(inputTuple);
		if (values != null) {
			for (Tuple value : values) {
				value.copyTo(tupleBucket);
				for (int j = 0; j < lengthConcatFields; ++j) {
					tupleBucket.addRaw(inputTuple, concatFields[j]);
				}
				output.add(tupleBucket);
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
			throws Exception {
		map = null;
	}

}
