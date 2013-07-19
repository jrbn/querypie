package nl.vu.cs.querypie.storage.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheCollectionValues {

    private final Map<Integer, List<Entry>> cache = new HashMap<Integer, List<Entry>>();

    public static class Entry {
	public long id;
	Collection<Long> values;
    }

    public synchronized void putCollection(long id, Collection<Long> values) {

	// Check it is not already there
	if (getCollection(values) != null)
	    return;

	Entry entry = new Entry();
	entry.id = id;
	entry.values = values;
	int size = values.size();

	List<Entry> list = null;
	list = cache.get(size);
	if (list == null) {
	    list = Collections
		    .synchronizedList(new ArrayList<CacheCollectionValues.Entry>());
	    cache.put(size, list);
	}

	list.add(entry);
    }

    public Entry getCollection(Collection<Long> values) {
	int size = values.size();
	List<Entry> list = null;
	list = cache.get(size);

	if (list != null) {
	    // The list contains only elements that are of the same size.
	    for (int i = 0; i < list.size(); ++i) {
		Entry entry = list.get(i);
		if (entry.values.containsAll(values))
		    return entry;
	    }
	}

	return null;
    }
}
