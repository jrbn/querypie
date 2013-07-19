package nl.vu.cs.querypie.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;

public class CompositeTriplePattern extends TupleIterator {

    static final Logger log = LoggerFactory
	    .getLogger(CompositeTriplePattern.class);

    TupleIterator currentItr;
    public TupleIterator second = null;
    public TupleIterator third = null;
    boolean f = true;
    boolean s = true;

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
	boolean answer = currentItr.next();
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
}
