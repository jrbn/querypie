package nl.vu.cs.querypie.storage.memory;

import arch.utils.LongMap;

public class SingleTupleMap extends LongMap<CollectionTuples> implements
	TupleMap {

    private static final long serialVersionUID = -5010634654810700451L;

    @Override
    public CollectionTuples get(Object key) {
	return get(((MultiValue) key).values[0]);
    }

    @Override
    public CollectionTuples put(MultiValue key, CollectionTuples value) {
	return put(key.values[0], value);
    }

    @Override
    public CollectionTuples getLong(long l) {
	return get(l);
    }
}
