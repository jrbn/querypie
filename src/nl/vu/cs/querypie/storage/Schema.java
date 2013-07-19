package nl.vu.cs.querypie.storage;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.storage.disk.FilesInterface;
import nl.vu.cs.querypie.storage.disk.TripleFile;
import nl.vu.cs.querypie.storage.memory.SortedCollectionTuples;
import nl.vu.cs.querypie.storage.memory.Triple;
import nl.vu.cs.querypie.storage.memory.TupleSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.utils.Configuration;

public class Schema {

    static final Logger log = LoggerFactory.getLogger(Schema.class);

    private static Schema instance = new Schema();
    public static final int ALL_RESOURCES = -1;
    public static final int SCHEMA_SUBSET = -2;
    public static final int ANY_RESOURCES = -3;
    public static final int SET_THRESHOLD = -4;

    String closureDir = null;
    FilesInterface fi = null;
    Configuration conf = null;

    Map<String, TupleSet> cachePrecomputedJoins = new HashMap<String, TupleSet>();
    Map<String, long[]> cacheSinglePatterns = new HashMap<String, long[]>();
    Map<String, SortedCollectionTuples> cacheVarsSinglePatterns = new HashMap<String, SortedCollectionTuples>();
    Map<String, Map<Long, List<Long>>> cacheLists = new HashMap<String, Map<Long, List<Long>>>();
    // Pointers to set of values
    Map<Long, Collection<Long>> cacheSubsets = new HashMap<Long, Collection<Long>>();
    Map<String, Boolean> cacheIntersectionSets = new HashMap<String, Boolean>();

    Map<String, Integer> existingIDs = new HashMap<String, Integer>();
    int counter = SET_THRESHOLD;

    private Schema() {
    }

    public static Schema getInstance() {
	return instance;
    }

    public void init(Configuration conf, String closureDir, FilesInterface fi) {
	this.closureDir = closureDir;
	this.fi = fi;
	this.conf = conf;
    }

    public long[] readTriples(Pattern p) {

	String p_signature = p.getSignature();
	if (cacheSinglePatterns.containsKey(p_signature)) {
	    return cacheSinglePatterns.get(p_signature);
	}

	// Determine positions to filter
	int[] pos_to_check = null;
	String patternLocation = p.getLocation();

	if (p.isFilter()) {
	    List<Integer> ps = new ArrayList<Integer>();
	    int i = 0;
	    for (RDFTerm t : p.p) {
		if (t.getValue() >= 0) {
		    ps.add(i);
		}
		i++;
	    }
	    pos_to_check = new int[ps.size()];
	    i = 0;
	    for (Integer pos : ps) {
		pos_to_check[i++] = pos;
	    }
	}

	TripleFile[] files = fi.getListFiles(conf, closureDir + "/"
		+ patternLocation, false);
	if (files != null && files.length > 0) {
	    Set<Triple> l = new HashSet<Triple>();
	    if (p.isFilter()) {
		for (TripleFile file : files) {
		    file.open();
		    while (file.next()) {
			Triple t = new Triple();
			t.subject = file.getFirstTerm();
			t.predicate = file.getSecondTerm();
			t.object = file.getThirdTerm();
			boolean ok = true;
			for (int pos : pos_to_check) {
			    if (pos == 0 && t.subject != p.p[pos].getValue()) {
				ok = false;
				break;
			    } else if (pos == 1
				    && t.predicate != p.p[pos].getValue()) {
				ok = false;
				break;
			    } else if (pos == 2
				    && t.object != p.p[pos].getValue()) {
				ok = false;
				break;
			    }
			}
			if (ok)
			    l.add(t);
		    }
		    file.close();
		}
	    } else {
		for (TripleFile file : files) {
		    file.open();
		    while (file.next()) {
			Triple t = new Triple();
			t.subject = file.getFirstTerm();
			t.predicate = file.getSecondTerm();
			t.object = file.getThirdTerm();
			l.add(t);
		    }
		    file.close();
		}
	    }

	    long[] raw_list = new long[l.size() * 3];
	    int i = 0;

	    // Filter eventual duplicates

	    for (Triple triple : l) {
		raw_list[i++] = triple.subject;
		raw_list[i++] = triple.predicate;
		raw_list[i++] = triple.object;
	    }

	    cacheSinglePatterns.put(p_signature, raw_list);
	    return raw_list;
	}

	cacheSinglePatterns.put(p_signature, null);
	return null;
    }

    private synchronized long getUniqueID(Pattern[] patterns, int pos)
	    throws IOException {
	String locations = "[";
	if (patterns != null)
	    for (Pattern p : patterns) {
		locations += p.getLocation() + ",";
	    }
	locations += "]";
	String key = locations + "-" + pos;
	if (!existingIDs.containsKey(key)) {
	    existingIDs.put(key, counter--);
	}

	if (counter <= RDFTerm.THRESHOLD_VARIABLE) {
	    throw new IOException(
		    "Too many values! They conflict with custom variables!");
	}

	return existingIDs.get(key);
    }

    public synchronized long calculateIdToSetAllElements(
	    TupleSet precomp_patterns, Pattern[] patterns,
	    int precomp_pos_shrd_generic) throws Exception {
	long id = getUniqueID(patterns, precomp_pos_shrd_generic);

	if (!cacheSubsets.containsKey(id)) {
	    Collection<Long> set = precomp_patterns.getAllValues(
		    precomp_pos_shrd_generic, true);
	    cacheSubsets.put(id, set);
	}

	return id;
    }

    public boolean chekValueInSet(RDFTerm idSet, long value,
	    ActionContext context) {
	Collection<Long> col = getSubset(idSet.getValue(), context);
	return col.contains(value);
    }

    public Collection<Long> getSubset(long v, ActionContext context) {
	if (!cacheSubsets.containsKey(v)) {
	    return getCustomSubset(v, context);
	} else {
	    return cacheSubsets.get(v);
	}
    }

    public Collection<Long> getSubset(long value) {
	return cacheSubsets.get(value);
    }

    public Iterator<Long> getSubsetItr(long v) {
	Iterable<Long> col = getSubset(v);
	return col.iterator();
    }

    @SuppressWarnings("unchecked")
    public Collection<Long> getCustomSubset(long v, ActionContext context) {
	return (Collection<Long>) context.getObjectFromCache(v);
    }

    @SuppressWarnings("unchecked")
    public Collection<Long> getSubset(long v, Context context, int submissionID) {
	if (!cacheSubsets.containsKey(v)) {
	    if (log.isDebugEnabled()) {
		log.debug("Schema2.getSubset: poolIds does not contain " + v);
	    }
	    return (Collection<Long>) context.getSubmissionCache()
		    .getObjectFromCache(submissionID, v);
	} else {
	    if (log.isDebugEnabled()) {
		log.debug("Schema2.getSubset: poolIds contains " + v);
	    }
	    return cacheSubsets.get(v);
	}
    }

    public boolean isIntersection(long schema1, long schema2,
	    ActionContext context) {

	Boolean response = null;
	String s = null;

	if (schema1 > RDFTerm.THRESHOLD_VARIABLE
		&& schema2 > RDFTerm.THRESHOLD_VARIABLE) {
	    if (schema1 < schema2)
		s = schema1 + "-" + schema2;
	    else
		s = schema2 + "-" + schema1;

	    response = cacheIntersectionSets.get(s);
	}

	if (response == null) {

	    Collection<Long> c1 = getSubset(schema1, context);
	    Collection<Long> c2 = getSubset(schema2, context);

	    boolean resp = false;
	    if (c1.size() < c2.size())
		for (long v1 : c1) {
		    if (c2.contains(v1)) {
			resp = true;
			break;
		    }
		}
	    else {
		for (long v2 : c2) {
		    if (c1.contains(v2)) {
			resp = true;
			break;
		    }
		}
	    }

	    if (schema1 > RDFTerm.THRESHOLD_VARIABLE
		    && schema2 > RDFTerm.THRESHOLD_VARIABLE) {
		synchronized (cacheIntersectionSets) {
		    if (cacheIntersectionSets.get(s) == null) {
			cacheIntersectionSets.put(s, resp);
		    }
		}
	    }
	    return resp;
	}

	return response.booleanValue();
    }

    public TupleSet joinPrecomputedPatterns(Pattern[] patterns,
	    String[] destination, int pos_recursive_pattern) {
	try {

	    String sign = "";
	    for (Pattern p : patterns) {
		sign += p.toString() + "-";
	    }

	    if (cachePrecomputedJoins.containsKey(sign)) {
		return cachePrecomputedJoins.get(sign);
	    }

	    List<Pattern> p = new ArrayList<Pattern>();
	    List<long[]> p_triples = new ArrayList<long[]>();
	    for (int i = 0; i < patterns.length; ++i) {
		Pattern pattern = patterns[i];
		// Read location
		long[] triples = readTriples(pattern);
		if (triples == null) {
		    return null;
		}
		p.add(pattern);
		p_triples.add(triples);
	    }

	    // Read content locations
	    TupleSet set = TupleSet.join2(p, p_triples, destination,
		    pos_recursive_pattern);

	    cachePrecomputedJoins.put(sign, set);
	    return set;
	} catch (Exception e) {
	    log.error("Error in the calculation of the patterns", e);
	}

	return null;
    }

    public boolean isPrecomputedPattern(Pattern p) {
	return cacheSinglePatterns.containsKey(p.getSignature());
    }

    public SortedCollectionTuples getVarsPrecomputedPattern(Pattern p)
	    throws Exception {
	String signature = p.getSignature();

	if (!cacheVarsSinglePatterns.containsKey(signature)) {
	    long[] triples = cacheSinglePatterns.get(p.getSignature());
	    if (triples == null) {
		triples = readTriples(p);
	    }
	    if (triples != null) {
		TupleSet set = new TupleSet();
		set = set.join(p, triples, null, null);
		cacheVarsSinglePatterns.put(signature, set.getAllValues());
	    } else {
		cacheVarsSinglePatterns.put(signature, null);
	    }
	}
	return cacheVarsSinglePatterns.get(signature);
    }

    private Map<Long, List<Long>> readList(String location) throws IOException,
	    Exception {
	Map<Long, List<Long>> map = new HashMap<Long, List<Long>>();

	File file = new File(closureDir + fi.getFilesSeparator() + location
		+ "/list");
	if (!file.exists()) {
	    return map;
	}

	DataInputStream is = new DataInputStream(new GZIPInputStream(
		fi.createInputStream(file)));
	try {
	    while (true) {
		long head = is.readLong();
		int nlists = is.readInt();
		while (nlists-- > 0) {
		    int sizeList = is.readInt();
		    List<Long> l = new ArrayList<Long>(sizeList);
		    while (sizeList-- > 0) {
			long value = is.readLong();
			l.add(value);
		    }
		    map.put(head, l);
		}
	    }
	} catch (EOFException e) {
	} catch (Exception e) {
	    log.error("Error", e);
	}
	is.close();

	return map;
    }

    public Map<Long, List<Long>> getAllList(String list_loc)
	    throws IOException, Exception {
	Map<Long, List<Long>> map = cacheLists.get(list_loc);
	if (map == null) {
	    map = readList(list_loc);
	    cacheLists.put(list_loc, map);
	}
	return map;
    }

    public void clear(boolean all) {
	if (all) {
	    cacheLists.clear();
	    cacheSinglePatterns.clear();
	}

	cachePrecomputedJoins.clear();
	cacheVarsSinglePatterns.clear();
	cacheSubsets.clear();
	cacheIntersectionSets.clear();
    }

    public Map<String, long[]> getCacheSinglePatterns() {
	return cacheSinglePatterns;
    }

    public void setCacheSinglePatterns(Map<String, long[]> cacheSinglePatterns) {
	this.cacheSinglePatterns = cacheSinglePatterns;
    }

    public Map<String, Map<Long, List<Long>>> getCacheLists() {
	return cacheLists;
    }

    public void setCacheLists(Map<String, Map<Long, List<Long>>> cacheLists) {
	this.cacheLists = cacheLists;
    }

    public void updateCacheSinglePatterns(String key, long[] newSize) {
	cacheSinglePatterns.put(key, newSize);
    }

    public void updateCacheLists(String loc, Map<Long, List<Long>> values) {
	cacheLists.put(loc, values);
    }
}
