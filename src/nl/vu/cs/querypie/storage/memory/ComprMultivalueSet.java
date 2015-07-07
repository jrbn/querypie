package nl.vu.cs.querypie.storage.memory;

import java.util.TreeSet;



public class ComprMultivalueSet {
    
    long[] buffer;
    
    public ComprMultivalueSet(TreeSet<MultiValue> uk, int sizeTuple) {
	buffer = new long[uk.size() * sizeTuple];
	//Serialize all the elements in a big array
	int c = 0;
	for(MultiValue v : uk) {
	    for(long el : v.values) {
		buffer[c++] = el;
	    }
	}
    }

    public boolean contains(MultiValue v) {
	//TODO
	return false;
    }

}