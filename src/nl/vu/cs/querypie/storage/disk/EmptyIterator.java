package nl.vu.cs.querypie.storage.disk;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.querypie.storage.TripleIterator;

public class EmptyIterator extends TupleIterator implements TripleIterator {

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

	@Override
	public long estimateRecords() throws Exception {
		return 0;
	}

	@Override
	public void stopReading() {	
	}
}