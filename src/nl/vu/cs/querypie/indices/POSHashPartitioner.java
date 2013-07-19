package nl.vu.cs.querypie.indices;

import arch.actions.Partitioner;
import arch.data.types.Tuple;

public class POSHashPartitioner extends Partitioner {

        /*
	RDFTerm s = new RDFTerm();
	RDFTerm p = new RDFTerm();
	RDFTerm o = new RDFTerm();
	*/

	@Override
	protected int partition(Tuple tuple, int nnodes) throws Exception {
	    return Math.abs(tuple.getHash(24) % nnodes);
	    /*
		tuple.get(s, p, o);
		return (p.toString() + " " + o.toString() + " " + s.toString())
				.hashCode() % nnodes;
	    */
	}

}
