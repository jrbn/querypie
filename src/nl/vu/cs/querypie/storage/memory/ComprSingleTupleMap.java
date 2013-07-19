package nl.vu.cs.querypie.storage.memory;

import java.util.ArrayList;
import java.util.Arrays;


import arch.utils.LongMap;

public class ComprSingleTupleMap implements TupleMap {

    int[] table;
    long[] values;
    int sizeSingleTuple = -1;

    public ComprSingleTupleMap(SingleTupleMap orig) {
	arch.utils.LongMap.Entry<CollectionTuples>[] table = orig.getTable();

	// Fill the table with the hashcodes
	this.table = new int[table.length];
	Arrays.fill(this.table, -1);

	ArrayList<Long> values = new ArrayList<Long>();

	for (arch.utils.LongMap.Entry<CollectionTuples> entry : table) {
	    if (entry != null) {
		int hash = -1;
		int pos = values.size();
		hash = LongMap.hash(entry.getKey());
		// At the position indicated by the hashcode set the position
		int i = hash & (table.length - 1);
		this.table[i] = pos;

		do {
		    // Insert position to the next entry and the number of
		    // elements
		    CollectionTuples value = entry.getValue();

		    // Insert a long with the information about the collection
		    values.add(entry.getKey());
		    long next_pos = value.getNTuples() << 1;
		    if (entry.getNext() == null) {
			next_pos += 1;
		    }
		    values.add(next_pos);

		    if (sizeSingleTuple == -1) {
			sizeSingleTuple = value.getSizeTuple();
		    }

		    // Insert the elements
		    for (int j = value.getStart(); j < value.getEnd(); ++j) {
			values.add(value.getRawValues()[j]);
		    }
		    entry = entry.getNext();

		} while (entry != null);
	    }
	}

	// Convert values in a long array
	this.values = new long[values.size()];
	for (int i = 0; i < values.size(); ++i) {
	    this.values[i] = values.get(i);
	}
    }

    @Override
    public CollectionTuples get(Object obj) {
	long key = ((MultiValue) obj).values[0];
	return getLong(key);
    }

    public CollectionTuples getLong(long key) {
	int hash = LongMap.hash(key);

	int pos = table[hash & (table.length - 1)];

	if (pos == -1)
	    return null;

	// Go to the right location
	while (values[pos] != key) {
	    if ((values[pos + 1] & 0x1) == 1) // No more tuples
		return null;
	    pos += 2 + sizeSingleTuple * (values[pos + 1] >> 1);
	}

	// Identified the correct location
	int n_tuples = (int) (values[pos + 1] >> 1);

	return new CollectionTuples(sizeSingleTuple, values, pos + 2, n_tuples);

    }

    @Override
    public CollectionTuples put(MultiValue key, CollectionTuples value)
	    throws Exception {
	throw new Exception("Not possible!");
    }
}