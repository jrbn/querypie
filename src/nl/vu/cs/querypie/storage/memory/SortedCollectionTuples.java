package nl.vu.cs.querypie.storage.memory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class SortedCollectionTuples implements Collection<Long>,
	java.io.Serializable {

    private static class LongIterator implements Iterator<Long> {
	int count = 0;
	long[] tuples;

	public LongIterator(long[] tuples) {
	    this.tuples = tuples;
	}

	@Override
	public boolean hasNext() {
	    return count < tuples.length;
	}

	@Override
	public Long next() {
	    return tuples[count++];
	}

	@Override
	public void remove() {
	}
    }

    static final Logger log = LoggerFactory
	    .getLogger(SortedCollectionTuples.class);

    long[] tuples = null;
    int sizeTuple = 0;

    public SortedCollectionTuples(Set<MultiValue> tmpSet) {
	if (tmpSet != null && tmpSet.size() > 0) {

	    // Fill the tuple array
	    int count = 0;
	    for (MultiValue value : tmpSet) {
		if (tuples == null) {
		    sizeTuple = value.values.length;
		    tuples = new long[sizeTuple * tmpSet.size()];
		}

		for (int i = 0; i < sizeTuple; ++i)
		    tuples[count++] = value.values[i];
	    }
	}
    }

    public void get(MultiValue value, int pos) {
	int start = pos * sizeTuple;
	for (int i = 0; i < sizeTuple; ++i) {
	    value.values[i] = tuples[start + i];
	}
    }

    public SortedCollectionTuples(Collection<Long> tmpSet) {
	sizeTuple = 1;
	tuples = new long[tmpSet.size()];
	int count = 0;
	for (long value : tmpSet) {
	    tuples[count++] = value;
	}
    }

    public boolean contains(long value) {
	return Arrays.binarySearch(tuples, value) >= 0;
    }

    public boolean contains(MultiValue head_vars) {
	if (tuples == null)
	    return false;

	// Do binary search. Possibly not bugged.
	int begin = 0;
	int end = tuples.length / sizeTuple - 1;

	while (begin <= end) {
	    int new_pos = (begin + end) >>> 1;
	    // Compare the value with head_vars
	    int compare = MultiValue.compare(head_vars.values, 0, tuples,
		    new_pos * sizeTuple, sizeTuple);
	    if (compare < 0) {
		end = new_pos - 1;
	    } else if (compare > 0) {
		begin = new_pos + 1;
	    } else {
		return true;
	    }
	}

	// Debug code: do a linear scan to make sure the algorithm is correct!
	// for(int i = 0; i < tuples.length; i+= sizeTuple) {
	//
	// boolean different = false;
	// for(int j = 0; j < sizeTuple && !different; ++j) {
	// if (tuples[i+j] != head_vars.values[j]) {
	// different = true;
	// break;
	// }
	// }
	//
	// if (!different) {
	// System.out.println("Error in the binary search; found value at " + i
	// + ", sizeTuple = " + sizeTuple
	// + ", tuples.length = " + tuples.length);
	// // contains(head_vars);
	// }
	// }

	return false;
    }

    @Override
    public Iterator<Long> iterator() {
	return new LongIterator(tuples);
    }

    @Override
    public int size() {
	return tuples.length / sizeTuple;
    }

    @Override
    public boolean contains(Object o) {
	return contains(((Long) o).longValue());
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if (! (c instanceof SortedCollectionTuples)) {
            return false;
        }
        SortedCollectionTuples t = (SortedCollectionTuples) c;

        if (tuples == null) {
            return t.tuples == null || t.tuples.length == 0;
        }

        if (sizeTuple != 1 || t.sizeTuple != 1) {
            log.error("This should not happen!", new Throwable());
            return false;
        }

        int index = 0;
        for (int i = 0; i < t.tuples.length; i++) {
            while (index < tuples.length && tuples[index] < t.tuples[i]) {
                index++;
            }
            if (index >= tuples.length || tuples[index] > t.tuples[i]) {
                return false;
            }
        }
        return true;
    }

    /****** NOT IMPLEMENTED ******/

    @Override
    public boolean add(Long e) {
	log.error("This should not happen!", new Throwable());
	return false;
    }

    @Override
    public boolean addAll(Collection<? extends Long> c) {
	log.error("This should not happen!", new Throwable());
	return false;
    }

    @Override
    public void clear() {
	log.error("This should not happen!", new Throwable());
    }

    @Override
    public boolean isEmpty() {
	log.error("This should not happen!", new Throwable());
	return false;
    }

    @Override
    public boolean remove(Object o) {
	log.error("This should not happen!", new Throwable());
	return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
	log.error("This should not happen!", new Throwable());
	return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
	log.error("This should not happen!", new Throwable());
	return false;
    }

    @Override
    public Object[] toArray() {
	log.error("This should not happen!", new Throwable());
	return null;
    }

    @Override
    public <T> T[] toArray(T[] a) {
	log.error("This should not happen!", new Throwable());
	return null;
    }

}
