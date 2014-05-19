package nl.vu.cs.querypie.storage.memory.tuplewrapper;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.storage.RDFTerm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDataTupleWrapper implements TupleWrapper {

    static final Logger log = LoggerFactory
	    .getLogger(SimpleDataTupleWrapper.class);

    public Tuple tuple;

    @Override
    public long get(int pos) throws Exception {
	return ((RDFTerm) (tuple.get(pos))).getValue();
    }
}
