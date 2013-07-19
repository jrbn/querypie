package nl.vu.cs.querypie.storage.disk;

import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;

public class EmptyIterator extends TupleIterator {

    @Override
    public boolean next() throws Exception {
	return false;
    }

    @Override
    public void getTuple(Tuple tuple) throws Exception {
    }

    @Override
    public boolean isReady() {
	return true;
    }
}