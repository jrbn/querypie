package nl.vu.cs.querypie.storage.memory;

public class CollectionTuples {

    private int sizeTuple;
    private long[] rawCollection = null;
    private int start = 0;
    private int end = 0;
    private int n_tuples = 0;

    public CollectionTuples(int sizeTuple, long[] values) {
	this.sizeTuple = sizeTuple;
	rawCollection = values;
	start = 0;
	n_tuples = values.length / sizeTuple;
	end = start + n_tuples * sizeTuple;
    }

    public CollectionTuples(int sizeTuple, long[] values, int start,
	    int n_tuples) {
	this.sizeTuple = sizeTuple;
	rawCollection = values;
	this.start = start;
	this.n_tuples = n_tuples;
	end = start + n_tuples * sizeTuple;
    }

    public int getSizeTuple() {
	return sizeTuple;
    }

    public int getNTuples() {
	return n_tuples;
    }

    public long getValue(int nTuple, int pos) {
	return rawCollection[start + nTuple * sizeTuple + pos];
    }

    public int getStart() {
	return start;
    }

    public int getEnd() {
	return end;
    }

    public long[] getRawValues() {
	return rawCollection;
    }
}