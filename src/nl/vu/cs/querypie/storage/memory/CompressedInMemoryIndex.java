package nl.vu.cs.querypie.storage.memory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressedInMemoryIndex implements Map<Long, Collection<Triple>>,Serializable {

    private static final long serialVersionUID = 5200440871226095310L;

    static final Logger log = LoggerFactory
	    .getLogger(CompressedInMemoryIndex.class);

    long[] keys = null;
    int[] starts = null;
    int[] lengths = null;
    Triple[] triples = null;

    public CompressedInMemoryIndex(long[] keys, int[] starts, int[] lengths,
	    Triple[] triples) {
	this.keys = keys;
	this.starts = starts;
	this.lengths = lengths;
	this.triples = triples;
    }
    
    public CompressedInMemoryIndex() {
    }

    @Override
    public void clear() {
	keys = null;
	starts = lengths = null;
	triples = null;
    }

    @Override
    public boolean containsKey(Object key) {
	Long k = (Long) key;
	return Arrays.binarySearch(keys, k.longValue()) >= 0;
    }

    @Override
    public boolean containsValue(Object value) {
	log.error("Not supported");
	System.exit(1);
	return false;
    }

    @Override
    public Set<java.util.Map.Entry<Long, Collection<Triple>>> entrySet() {
	log.error("Not supported");
	System.exit(1);
	return null;
    }

    @Override
    public Collection<Triple> get(Object key) {
	if (keys == null) {
	    return null;
	}
	Long k = (Long) key;
	int pos = Arrays.binarySearch(keys, k.longValue());
	if (pos >= 0) {
	    return Arrays.asList(Arrays.copyOfRange(triples, starts[pos],
		    starts[pos] + lengths[pos]));
	}
	return null;
    }

    @Override
    public boolean isEmpty() {
	return keys == null || keys.length == 0;
    }

    @Override
    public Set<Long> keySet() {
	log.error("Not supported");
	System.exit(1);
	return null;
    }

    @Override
    public Collection<Triple> put(Long key, Collection<Triple> value) {
	log.error("Not supported");
	System.exit(1);
	return null;
    }

    @Override
    public void putAll(Map<? extends Long, ? extends Collection<Triple>> m) {
	log.error("Not supported");
	System.exit(1);
    }

    @Override
    public Collection<Triple> remove(Object key) {
	log.error("Not supported");
	System.exit(1);
	return null;
    }

    @Override
    public int size() {
	return keys.length;
    }

    @Override
    public Collection<Collection<Triple>> values() {
	log.error("Not supported");
	System.exit(1);
	return null;
    }

    public Triple[] getTriples() {
	return triples;
    }

}