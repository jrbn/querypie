package arch.actions.joins;

import arch.ActionContext;
import arch.actions.Action;
import arch.buckets.BucketIterator;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;
import arch.storage.container.WritableContainer;

public class MergeJoin extends Action {

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
	    WritableContainer<Chain> chainsToResolve, WritableContainer<Chain> chainsToSend) throws Exception {
	return applyTo(context, tuple, inputChain, chainsToResolve);
    }

    public static Chain applyTo(ActionContext context, Tuple tuple,
	    Chain inputChain, WritableContainer<Chain> childrenChains)
	    throws Exception {

	TInt bucketID = new TInt();
	TInt lengthJoin = new TInt();
	tuple.get(bucketID, lengthJoin);

	Object[] params = new Object[2 + lengthJoin.getValue() * 2];
	params[0] = bucketID.getValue();
	params[1] = lengthJoin.getValue();

	TInt index = new TInt();
	for (int i = 0; i < lengthJoin.getValue(); ++i) {
	    tuple.get(index, 2 + i);
	    params[2 + i] = index.getValue();
	}

	for (int i = 0; i < lengthJoin.getValue(); ++i) {
	    tuple.get(index, 2 + lengthJoin.getValue() + i);
	    params[2 + lengthJoin.getValue() + i] = index.getValue();
	}

	inputChain.addAction(MergeJoin.class.getName(), null, params);
	return inputChain;

    }

    TupleIterator itr = null;
    int lengthJoins = 0;
    int[] bucketIndexJoins = new int[10];
    int[] inputIndexJoins = new int[10];
    int lengthConcatFields = 0;
    int[] concatFields = new int[20];

    boolean itrOk, first;
    WritableContainer<Tuple> bag = new WritableContainer<Tuple>(false, false,
	    true, 4096); // 4K
    Tuple tupleBucket = new Tuple();
    Tuple tmpTuple = new Tuple();

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {

	// Set the indexes of the fields to be joined
	lengthJoins = (Integer) params[1];
	for (int i = 0; i < lengthJoins; ++i) {
	    bucketIndexJoins[i] = (Integer) params[2 + i];
	    inputIndexJoins[i] = (Integer) params[2 + lengthJoins + i];
	}

	// Init the bucket
	int bucketID = (Integer) params[0];
	itr = context.getTuplesBuckets().getIterator(chain.getSubmissionId(),
		bucketID);
	bag.clear();
	itrOk = itr.next();
	if (itrOk) {
	    itr.getTuple(tupleBucket);
	}
	first = true;
    }

    private int compare(Tuple t1, Tuple t2, int nFields, int[] f1, int[] f2)
	    throws Exception {
	int response = 0;
	for (int i = 0; i < nFields; ++i) {
	    response = t1.compareElement(f1[i], t2, f2[i]);
	    if (response != 0) {
		return response;
	    }
	}
	return response;
    }

    private void join(Tuple inputTuple, WritableContainer<Tuple> output)
	    throws Exception {
	// Perform the join between the elements in the bag and the
	// input tuple.
	int size = bag.getNElements();
	for (int i = 0; i < size; ++i) {
	    bag.get(tmpTuple, i);

	    for (int j = 0; j < lengthConcatFields; ++j) {
		tmpTuple.addRaw(inputTuple, concatFields[j]);
	    }
	    output.add(tmpTuple);
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

	// If the bag is not empty here, tupleBucket may refer to the tuple that
	// comes
	// after the bag (if itrOk is true). So, we need to process the bag
	// first.

	if (bag.getNElements() != 0) {
	    bag.get(tmpTuple, 0);
	    int response = compare(tmpTuple, inputTuple, lengthJoins,
		    bucketIndexJoins, inputIndexJoins);
	    if (response == 0) {
		join(inputTuple, output);
		return;
	    } else {
		bag.clear();
	    }
	}

	// If we get here, the bag is empty.
	if (itrOk) {
	    // compare the two tuples
	    int response = -1;
	    while ((response = compare(tupleBucket, inputTuple, lengthJoins,
		    bucketIndexJoins, inputIndexJoins)) < 0
		    && (itrOk = itr.next())) {
		itr.getTuple(tupleBucket);
	    }

	    if (response == 0) {
		// Fill the bag
		bag.add(tupleBucket);
		do {
		    itrOk = itr.next();
		    if (itrOk) {
			itr.getTuple(tmpTuple);
			response = compare(tupleBucket, tmpTuple, lengthJoins,
				bucketIndexJoins, bucketIndexJoins);

			if (response == 0
				&& tupleBucket.compareTo(tmpTuple) != 0) {
			    bag.add(tmpTuple);
			}
			tmpTuple.copyTo(tupleBucket);
		    }
		} while (itrOk && response == 0);

		join(inputTuple, output);
	    }
	}
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
	    throws Exception {
	if (itr != null) {
	    context.getTuplesBuckets().releaseIterator((BucketIterator) itr,
		    true);
	    itr = null;
	}
	bag.clear();
    }

}
