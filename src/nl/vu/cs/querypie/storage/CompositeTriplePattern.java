package nl.vu.cs.querypie.storage;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeTriplePattern extends TupleIterator implements
		TripleIterator {

	static final Logger log = LoggerFactory
			.getLogger(CompositeTriplePattern.class);

	TupleIterator currentItr;
	public TupleIterator second = null;
	public TupleIterator third = null;
	boolean f = true;
	boolean s = true;
	private boolean stop = false;

	public CompositeTriplePattern(TupleIterator first, TupleIterator second) {
		this.currentItr = first;
		this.second = second;
	}

	public CompositeTriplePattern(TupleIterator first, TupleIterator second,
			TupleIterator third) {
		this.currentItr = first;
		this.second = second;
		this.third = third;
	}

	@Override
	public boolean next() throws Exception {
		boolean answer = !stop && currentItr.nextTuple();
		if (!answer) {
			if (f) {
				// Move to second
				currentItr = second;
				f = false;
				return next();
			} else if (third != null && s) {
				currentItr = third;
				s = false;
				return next();
			}
		}
		return answer;
	}

	@Override
	public void getTuple(Tuple tuple) throws Exception {
		currentItr.getTuple(tuple);
	}

	@Override
	public boolean isReady() {
		return true;
	}

	@Override
	public long estimateRecords() throws Exception {
		long count = ((TripleIterator) currentItr).estimateRecords();
		if (second != null && second != currentItr) {
			count += ((TripleIterator) second).estimateRecords();
		}
		if (third != null && third != currentItr) {
			count += ((TripleIterator) third).estimateRecords();
		}
		if (count > Integer.MAX_VALUE) {
			count = Integer.MAX_VALUE;
		}
		return count;
	}

	@Override
	public void stopReading() {
		stop = true;
	}
}
