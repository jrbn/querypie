package nl.vu.cs.querypie.storage.memory.tuplewrapper;


public interface TupleWrapper {
    public long get(int pos) throws Exception;
}