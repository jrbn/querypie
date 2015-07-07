package nl.vu.cs.querypie.storage.memory.tuplewrapper;


public class ArrayTupleWrapper implements TupleWrapper {

    public long[] values;

    @Override
    public long get(int pos) {
	return values[pos];
    }
}