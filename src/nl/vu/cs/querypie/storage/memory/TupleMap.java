package nl.vu.cs.querypie.storage.memory;


public interface TupleMap {

    public CollectionTuples get(Object key);

    public CollectionTuples put(MultiValue key, CollectionTuples value)
	    throws Exception;

    public CollectionTuples getLong(long l);

}