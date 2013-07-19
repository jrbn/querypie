package nl.vu.cs.querypie.storage.memory.tuplewrapper;

import nl.vu.cs.querypie.storage.RDFTerm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.Tuple;

public class SimpleDataTupleWrapper implements TupleWrapper {

    static final Logger log = LoggerFactory
	    .getLogger(SimpleDataTupleWrapper.class);

    private RDFTerm term;
    public Tuple tuple;

    public SimpleDataTupleWrapper(RDFTerm term) {
	this.term = term;
    }

    @Override
    public long get(int pos) throws Exception {
        tuple.get(term, pos);
	return term.getValue();
    }
}
