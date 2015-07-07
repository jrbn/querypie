package nl.vu.cs.querypie.storage.memory.tuplewrapper;

import nl.vu.cs.querypie.storage.RDFTerm;

public class RDFTermTupleWrapper implements TupleWrapper {

    public RDFTerm[] values;

    @Override
    public long get(int pos) {
	return values[pos].getValue();
    }

}