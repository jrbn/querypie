package nl.vu.cs.querypie.storage.memory;

import java.util.HashMap;



public class MultipleTupleMap extends HashMap<MultiValue, CollectionTuples>
	implements TupleMap {

    private static final long serialVersionUID = -5111517998415442067L;

    @Override
    public CollectionTuples getLong(long l) {
	return null;
    }
}