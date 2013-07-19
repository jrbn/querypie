package nl.vu.cs.querypie.storage.disk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;
import nl.vu.cs.querypie.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;
import arch.utils.Consts;
import arch.utils.LongMap;

public class PatternIterator extends TupleIterator {

    public static final int MAX_ENTRY_SIZE = 4 * 1024 * 1024; // 4M
    public static final long MAX_ENTRIES_CACHE_SIZE = 4000L * 1024 * 1024;

    static final Logger log = LoggerFactory.getLogger(PatternIterator.class);

    public long ntuples;

    long entry1;
    long entry2;
    long entry3;

    int[] positions;

    long output1, output2, output3;
    RDFTerm[] array = new RDFTerm[3];
    RDFTerm v1, v2, v3;

    byte[] currentBlock = null;

    Iterator<Long> itrSchema = null;
    Index index = null;

    int dummy = -1;
    int currentBlockIndex = 0;
    int currentOffset = 0;
    int coordinates[] = new int[3];

    Iterator<Long> customItr = null;
    Schema schema;
    ActionContext context;

    // We also create a cache that, for each entry1, maintains all info
    // about it.
    // It also is in the form of a hashmap, mapping the entry1 value to an
    // array
    // of Entry2Info objects.

    public static class EntryInfo {
	int size;
	int index;
	EntryInfo next;
	EntryInfo prev;
	Entry2Info[] entries2;
	public long key;

	public EntryInfo(long key, Entry2Info[] entries2, int size, int index) {
	    this.key = key;
	    this.entries2 = entries2;
	    this.size = size;
	    this.index = index;
	}
    }

    public static class Entry2Info {
	public long entry2;
	public long entries3[];
    }

    private int entry2Index;
    private int entry3Index;
    private EntryInfo currentEntryInfo;

    @SuppressWarnings("unchecked")
    public static LongMap<EntryInfo>[] entryHashMaps = new LongMap[] {
	    new LongMap<EntryInfo>(), new LongMap<EntryInfo>(),
	    new LongMap<EntryInfo>(), new LongMap<EntryInfo>(),
	    new LongMap<EntryInfo>(), new LongMap<EntryInfo>(),
	    new LongMap<EntryInfo>(), new LongMap<EntryInfo>(),
	    new LongMap<EntryInfo>(), new LongMap<EntryInfo>() };

    public static EntryInfo entryQueueHead;
    public static EntryInfo entryQueueTail;

    protected int myId;
    private int nextBlockIndexEntry2;
    private int nextOffsetEntry2;

    private static long totalEntryCacheSize;

    public PatternIterator() {
	array[0] = new RDFTerm();
	array[1] = new RDFTerm();
	array[2] = new RDFTerm();
    }

    long[] tuple = new long[3];
    private InMemoryTripleContainer checkDuplications;

    public void init(Index index, Schema schema, long[] tuple,
	    ActionContext context, long minValue, long maxValue,
	    InMemoryTripleContainer input) throws Exception {
	time = System.currentTimeMillis();
	ntuples = 0;

	this.checkDuplications = input;

	this.tuple[0] = tuple[0];
	this.tuple[1] = tuple[1];
	this.tuple[2] = tuple[2];

	this.context = context;
	currentBlock = null;
	dummy = -1;
	this.schema = schema;
	this.myId = index.getId();
	ntuples = 0;
	currentEntryInfo = null;

	this.index = index;
	this.positions = index.positions;
	entry1 = tuple[positions[0]];
	entry2 = tuple[positions[1]];
	entry3 = tuple[positions[2]];

	v1 = array[positions[0]];
	v2 = array[positions[1]];
	v3 = array[positions[2]];

	if (entry1 <= Schema.SET_THRESHOLD) {
	    if (entry1 <= RDFTerm.THRESHOLD_VARIABLE) {
		customItr = getIterator(entry1, minValue, maxValue);
	    } else {
		customItr = schema.getSubsetItr(entry1);
	    }
	    if (customItr.hasNext()) {
		long value = customItr.next();
		if (value >= minValue && value <= maxValue) {
		    setup(value);
		} else if (log.isDebugEnabled()) {
		    log.debug("No suitable value");
		}
	    }
	} else {
	    customItr = null;
	    if (entry1 >= minValue && entry1 <= maxValue) {
		setup(entry1);
	    }
	}
    }

    @SuppressWarnings("unchecked")
    protected Iterator<Long> getIterator(long entry, long minValue,
	    long maxValue) {
	Collection<Long> s = (Collection<Long>) context
		.getObjectFromCache(entry);
	if (s == null) {
	    log.error("Failed getting iterator for " + entry);
	    return (new ArrayList<Long>()).iterator();
	}
	if (log.isDebugEnabled()) {
	    log.debug("variable " + entry + " has " + s.size() + " elements");
	}
	ArrayList<Long> l;
	synchronized (this.getClass()) {
	    l = (ArrayList<Long>) context.getObjectFromCache("it-" + entry);
	    if (l == null || l.size() != s.size()) {
		l = new ArrayList<Long>(s);
		context.putObjectInCache("it-" + entry, l);
	    }
	}

	int lo = 0;
	int hi = l.size() - 1;
	if (l.get(lo) >= minValue && l.get(hi) <= maxValue) {
	    if (log.isDebugEnabled()) {
		log.debug("Returning iterator of size " + l.size() + " for "
			+ entry);
	    }
	    return l.iterator();
	}
	while (lo < hi) {
	    int mid = (int) Math.ceil((double) (lo + hi) / 2);
	    long val = l.get(mid);
	    if (val < minValue) {
		lo = mid + 1;
	    } else if (val > minValue) {
		hi = mid - 1;
	    } else {
		lo = hi = mid;
	    }
	}
	int lo1 = lo;
	hi = l.size() - 1;
	while (lo1 < hi) {
	    int mid = (int) Math.ceil((double) (lo1 + hi) / 2);
	    long val = l.get(mid);
	    if (val > maxValue) {
		hi = mid - 1;
	    } else if (val < maxValue) {
		lo1 = mid + 1;
	    } else {
		lo1 = hi = mid;
	    }
	}
	List<Long> l1 = l.subList(lo, hi + 1);
	if (log.isDebugEnabled()) {
	    log.debug("Returning iterator of size " + l1.size() + " for "
		    + entry);
	}
	return l1.iterator();
    }

    protected void setup(long entry1) throws Exception {
	output1 = entry1;
	output2 = -1;
	output3 = -1;
	entry2Index = 0;
	entry3Index = 0;
	currentBlock = null;
	synchronized (entryHashMaps) {
	    currentEntryInfo = entryHashMaps[myId].get(entry1);
	    if (currentEntryInfo != null) {
		if (currentEntryInfo != entryQueueTail) {
		    moveToEndOfQueue(currentEntryInfo);
		}
	    }
	}

	if (currentEntryInfo == null
		&& index.getInitialAddress(entry1, coordinates, context)) {
	    currentBlockIndex = coordinates[0];
	    currentOffset = coordinates[1];
	    if (coordinates[2] < MAX_ENTRY_SIZE) {
		synchronized (entryHashMaps) {
		    currentEntryInfo = entryHashMaps[myId].get(entry1);
		    if (currentEntryInfo == null) {
			if (log.isDebugEnabled()) {
			    log.debug("Creating EntryInfo for entry " + entry1);
			}
			currentBlock = index.getBlock(currentBlockIndex,
				context);
			currentEntryInfo = new EntryInfo(entry1, readInfo(),
				coordinates[2], myId);
			entryHashMaps[myId].put(entry1, currentEntryInfo);
			totalEntryCacheSize += currentEntryInfo.size;
			while (totalEntryCacheSize > MAX_ENTRIES_CACHE_SIZE) {
			    EntryInfo e = entryQueueHead;
			    removeFromQueue(e);
			    entryHashMaps[e.index].remove(e.key);
			    if (log.isDebugEnabled()) {
				log.debug("Removing EntryInfo for entry "
					+ e.key + ",  size = " + e.size);
			    }
			    totalEntryCacheSize -= e.size;
			}
			appendToQueue(currentEntryInfo);
		    } else {
			if (log.isDebugEnabled()) {
			    log.debug("Using EntryInfo for entry " + entry1);
			}
			if (currentEntryInfo != entryQueueTail) {
			    moveToEndOfQueue(currentEntryInfo);
			}
		    }
		}
	    } else {
		if (log.isDebugEnabled()) {
		    log.debug("Not caching for entry " + entry1
			    + " size estimate = " + coordinates[2]);
		}
		currentEntryInfo = null;
		currentBlock = index.getBlock(currentBlockIndex, context);
	    }
	}
	if (currentEntryInfo != null || currentBlock != null) {
	    if (entry2 < Schema.ALL_RESOURCES) {
		if (entry2 <= RDFTerm.THRESHOLD_VARIABLE) {
		    itrSchema = getIterator(entry2, Long.MIN_VALUE,
			    Long.MAX_VALUE);
		} else {
		    itrSchema = schema.getSubsetItr((int) entry2);
		}
	    } else if (entry3 < Schema.ALL_RESOURCES) {
		if (entry3 <= RDFTerm.THRESHOLD_VARIABLE) {
		    itrSchema = getIterator(entry3, Long.MIN_VALUE,
			    Long.MAX_VALUE);
		} else {
		    itrSchema = schema.getSubsetItr((int) entry3);
		}
	    }
	}
    }

    private static void appendToQueue(EntryInfo e) {
	if (entryQueueTail == null) {
	    entryQueueTail = entryQueueHead = e;
	} else {
	    entryQueueTail.next = e;
	    e.prev = entryQueueTail;
	    e.next = null;
	    entryQueueTail = e;
	}
    }

    private static void removeFromQueue(EntryInfo e) {
	if (e == entryQueueHead) {
	    entryQueueHead = e.next;
	    if (e.next != null) {
		e.next.prev = null;
	    } else {
		entryQueueTail = null;
	    }
	} else {
	    e.prev.next = e.next;
	    if (e.next == null) {
		entryQueueTail = e.prev;
	    } else {
		e.next.prev = e.prev;
	    }
	}
    }

    private static void moveToEndOfQueue(EntryInfo e) {
	// Called when e is not the queue tail.
	e.next.prev = e.prev;
	if (e == entryQueueHead) {
	    entryQueueHead = e.next;
	} else {
	    e.prev.next = e.next;
	}
	entryQueueTail.next = e;
	e.prev = entryQueueTail;
	e.next = null;
	entryQueueTail = e;
    }

    public void init(ActionContext context) {
	ntuples = 0;
	dummy = 0;
	this.context = context;
    }

    @Override
    public String toString() {
	RDFStorage storage = (RDFStorage) context
		.getInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
	String[] text = storage.getText(tuple);
	String t = null;
	if (text != null) {
	    t = text[0] + "(" + tuple[0] + ") " + text[1] + "(" + tuple[1]
		    + ") " + text[2] + "(" + tuple[2] + ")";
	} else {
	    t = tuple[0] + " " + tuple[1] + " " + tuple[2];
	}
	return t
		+ (positions == null ? " [null]" : (" [" + positions[0] + " "
			+ positions[1] + " " + positions[2] + "]"));

	// return entry1
	// + " "
	// + entry2
	// + " "
	// + entry3
	// + (positions == null ? " [null]" : (" [" + positions[0] + " "
	// + positions[1] + " " + positions[2] + "]"));
    }

    long time;

    @Override
    public boolean next() throws Exception {
	if (dummy == 0) {
	    ++dummy;
	    return true;
	} else if (dummy == 1) {
	    return false;
	}
	for (;;) {
	    boolean value = currentEntryInfo != null ? nextInBlock(currentEntryInfo.entries2)
		    : nextInBlock();
	    if (!value && customItr != null && customItr.hasNext()) {
		// long time = System.nanoTime();
		// try {
		long v = customItr.next();
		setup(v);
		// } catch (Exception e) {
		// log.error("Error", e);
		// }
		// context.incrCounter(setupTimer,
		// System.nanoTime() - time);
		continue;
	    }
	    if (value) {
		ntuples++;

		v1.setValue(output1);
		v2.setValue(output2);
		v3.setValue(output3);

		if (checkDuplications != null
			&& checkDuplications.containsTriple(array)) {
		    continue;
		}
	    } else {
		checkDuplications = null;
	    }

	    return value;
	}
    }

    @Override
    public void getTuple(Tuple tuple) throws Exception {
	if (dummy != -1) {
	    return;
	}
	tuple.set(array);
    }

    private boolean gotoNextEntry2() throws IOException {
	if (nextBlockIndexEntry2 < 0) {
	    return false;
	}
	// if (log.isDebugEnabled()) {
	// log.debug("Go to next entry2");
	// }
	currentBlockIndex = nextBlockIndexEntry2;
	currentBlock = index.getBlock(currentBlockIndex, context);
	currentOffset = nextOffsetEntry2;
	output2 = -1; // to not read a separator in nextInBlock().
	return true;
    }

    private boolean getNextEntry2() throws IOException {
	output2 = readTerm();
	// if (log.isDebugEnabled()) {
	// log.debug("NextEntry2 : " + output[1]);
	// }
	if (entry2 != Schema.ALL_RESOURCES) {
	    long resourceToMatch;
	    if (entry2 >= 0) {
		resourceToMatch = entry2;
	    } else {
		if (itrSchema == null || !itrSchema.hasNext()) {
		    return false;
		}
		resourceToMatch = itrSchema.next();
	    }
	    boolean foundIntersection = false;
	    while (!foundIntersection) {
		if (output2 < resourceToMatch) {
		    output2 = goToNextPredicate();
		    // if (log.isDebugEnabled()) {
		    // log.debug("NextPredicate : " + output[1]);
		    // }
		    if (output2 == Long.MIN_VALUE) {
			// Reached the end of the stream.
			return false;
		    }
		    // if (log.isDebugEnabled()) {
		    // log.debug("Marking position for offset");
		    // }
		} else if (output2 > resourceToMatch) {
		    // Need to move the schema iterator
		    if (entry2 < Schema.ALL_RESOURCES) {
			if (!itrSchema.hasNext()) {
			    return false;
			}
			resourceToMatch = itrSchema.next();
		    } else {
			return false;
		    }
		} else {
		    foundIntersection = true;
		}
	    }
	} else if (entry3 < Schema.ALL_RESOURCES) {
	    // If entry2 is unbound, we should create a new iterator on the
	    // entry3's
	    // for each new value of entry2! --Ceriel
	    if (entry3 <= RDFTerm.THRESHOLD_VARIABLE) {
		itrSchema = getIterator(entry3, Long.MIN_VALUE, Long.MAX_VALUE);
	    } else {
		itrSchema = schema.getSubsetItr((int) entry3);
	    }
	}
	// Read position of next predicate.
	int[] position = readPosition();
	if (position[0] == Integer.MIN_VALUE) {
	    nextBlockIndexEntry2 = -1;
	    // if (log.isDebugEnabled()) {
	    // log.debug("Marking position: no more entries2");
	    // }
	} else {
	    nextBlockIndexEntry2 = position[0];
	    nextOffsetEntry2 = position[1];
	    // if (log.isDebugEnabled()) {
	    // log.debug("Marking position: blockno = " + nextBlockIndexEntry2
	    // + ", offset = " + nextOffsetEntry2);
	    // }
	}
	return true;
    }

    private boolean nextInBlock() {

	if (currentBlock == null) {
	    return false;
	}

	try {
	    byte flag = 0;
	    for (;;) {
		if (output2 != -1) {
		    flag = readSeparator();
		    if (flag == Index.FLAG_END_SEQUENCE
			    || (flag == Index.FLAG_NEXT_PREDICATE && entry2 >= 0)) {
			return false;
		    }
		}

		if (flag != Index.FLAG_NEXT_OBJECT) {
		    // Read the first entry
		    if (!getNextEntry2()) {
			return false;
		    }
		}

		output3 = readTerm(); // Read the third entry
		if (entry3 >= 0) {
		    if (entry3 == output3) {
			return true;
		    }
		    // Go and search for the right position
		    flag = readSeparator();
		    while (output3 < entry3 && flag == Index.FLAG_NEXT_OBJECT) {
			output3 = readTerm();
			if (output3 == entry3) {
			    return true;
			}
			flag = readSeparator();
		    }
		    if (!gotoNextEntry2()) {
			return false;
		    }
		    flag = 0;
		    continue;
		} else if (entry3 < Schema.ALL_RESOURCES) {
		    if (itrSchema == null || !itrSchema.hasNext()) {
			if (!gotoNextEntry2()) {
			    return false;
			}
			flag = 0;
			continue;
		    }
		    long toMatch = itrSchema.next();
		    if (output3 == toMatch) {
			return true;
		    }
		    flag = readSeparator();
		    for (;;) {
			if (output3 < toMatch && flag == Index.FLAG_NEXT_OBJECT) {
			    output3 = readTerm();
			    flag = readSeparator();
			} else if (output3 > toMatch && itrSchema.hasNext()) {
			    toMatch = itrSchema.next();
			} else {
			    break;
			}
		    }
		    if (output3 != toMatch) {
			if (!gotoNextEntry2()) {
			    return false;
			}
			flag = 0;
			continue;
		    } else {
			currentOffset--;
		    }
		}
		return true;
	    }
	} catch (IOException e) {
	    log.error("Error reading the files", e);
	    return false;
	}
    }

    private boolean nextInBlock(Entry2Info[] entryInfo) {

	for (;;) {
	    if (entry2Index >= entryInfo.length) {
		return false;
	    }
	    Entry2Info e = entryInfo[entry2Index];
	    if (output2 != e.entry2) {
		output2 = e.entry2;
		if (entry2 != Schema.ALL_RESOURCES) {
		    long resourceToMatch;
		    if (entry2 >= 0) {
			resourceToMatch = entry2;
		    } else {
			if (itrSchema == null || !itrSchema.hasNext()) {
			    return false;
			}
			resourceToMatch = itrSchema.next();
		    }
		    boolean foundIntersection = false;
		    while (!foundIntersection) {
			if (output2 < resourceToMatch) {
			    ++entry2Index;
			    entry3Index = 0;
			    if (entry2Index >= entryInfo.length) {
				return false;
			    }
			    e = entryInfo[entry2Index];
			    output2 = e.entry2;
			} else if (output2 > resourceToMatch) {
			    // Need to move the schema iterator
			    if (entry2 < Schema.ALL_RESOURCES) {
				if (!itrSchema.hasNext()) {
				    return false;
				}
				resourceToMatch = itrSchema.next();
			    } else {
				return false;
			    }
			} else {
			    foundIntersection = true;
			}
		    }
		} else if (entry3Index == 0 && entry3 < Schema.ALL_RESOURCES) {
		    // If entry2 is unbound, we should create a new iterator on
		    // the
		    // entry3's
		    // for each new value of entry2! --Ceriel
		    if (entry3 <= RDFTerm.THRESHOLD_VARIABLE) {
			itrSchema = schema.getSubset(entry3, context)
				.iterator();
		    } else {
			itrSchema = schema.getSubsetItr((int) entry3);
		    }
		}
	    }
	    output3 = e.entries3[entry3Index++];
	    if (entry3 >= 0) {
		while (output3 < entry3 && entry3Index < e.entries3.length) {
		    output3 = e.entries3[entry3Index++];
		}
		entry2Index++;
		entry3Index = 0;
		if (output3 == entry3) {
		    return true;
		}
		continue;
	    }
	    if (entry3 < Schema.ALL_RESOURCES) {
		if (itrSchema == null || !itrSchema.hasNext()) {
		    entry2Index++;
		    entry3Index = 0;
		    continue;
		}
		long toMatch = itrSchema.next();
		for (;;) {
		    if (output3 < toMatch && entry3Index < e.entries3.length) {
			output3 = e.entries3[entry3Index++];
		    } else if (output3 > toMatch && itrSchema.hasNext()) {
			toMatch = itrSchema.next();
		    } else {
			break;
		    }
		}
		if (output3 != toMatch) {
		    entry2Index++;
		    entry3Index = 0;
		    continue;
		}
	    }

	    if (entry3Index >= e.entries3.length) {
		entry2Index++;
		entry3Index = 0;
	    }

	    return true;
	}
    }

    private Entry2Info[] readInfo() {

	if (currentBlock == null) {
	    return null;
	}

	ArrayList<Entry2Info> result = new ArrayList<Entry2Info>();

	byte flag = Index.FLAG_NEXT_PREDICATE;
	while (flag == Index.FLAG_NEXT_PREDICATE) {
	    Entry2Info e = new Entry2Info();
	    result.add(e);
	    e.entry2 = readTerm();
	    if (log.isDebugEnabled()) {
		log.debug("read entry2 " + e.entry2);
	    }
	    // Skip the offset to the next predicate.
	    if (currentOffset > currentBlock.length - 8) {
		currentOffset = 8;
		currentBlock = index.getBlock(++currentBlockIndex, context);
	    } else {
		currentOffset += 8;
	    }
	    long[] entries3 = new long[4];
	    int numEntries3 = 0;
	    entries3[0] = readTerm();
	    if (log.isDebugEnabled()) {
		log.debug("Read entry3 : " + entries3[0]);
	    }
	    numEntries3++;

	    flag = readSeparator();
	    if (log.isDebugEnabled()) {
		log.debug("Read separator " + flag);
	    }
	    while (flag == Index.FLAG_NEXT_OBJECT) {
		if (numEntries3 >= entries3.length) {
		    long[] n = new long[entries3.length * 2];
		    System.arraycopy(entries3, 0, n, 0, entries3.length);
		    entries3 = n;
		}
		entries3[numEntries3++] = readTerm();
		if (log.isDebugEnabled()) {
		    log.debug("Read entry3 " + entries3[numEntries3 - 1]);
		}
		flag = readSeparator();
	    }
	    e.entries3 = new long[numEntries3];
	    System.arraycopy(entries3, 0, e.entries3, 0, numEntries3);
	}
	return result.toArray(new Entry2Info[result.size()]);
    }

    private long goToNextPredicate() throws IOException {
	int[] position = readPosition();
	if (position[0] == Integer.MIN_VALUE) {
	    return Long.MIN_VALUE;
	}
	goForward(position);
	return readTerm();
    }

    private void goForward(int[] position) {
	if (currentBlockIndex != position[0]) {
	    currentBlockIndex = position[0];
	    currentBlock = index.getBlock(currentBlockIndex, context);
	}
	currentOffset = position[1];
    }

    private byte readSeparator() {
	try {
	    return currentBlock[currentOffset++];
	} catch (ArrayIndexOutOfBoundsException e) {
	    currentOffset = 0;
	    currentBlock = index.getBlock(++currentBlockIndex, context);
	}
	return currentBlock[currentOffset++];
    }

    int[] temp = new int[2];
    int[] readPosition = new int[2];

    private int[] readPosition() {
	if (currentOffset > (currentBlock.length - 8)) {
	    currentOffset = 0;
	    currentBlock = index.getBlock(++currentBlockIndex, context);
	}
	readPosition[0] = Utils.decodeInt(currentBlock, currentOffset);
	readPosition[1] = Utils.decodeInt(currentBlock, currentOffset + 4);
	currentOffset += 8;
	if (readPosition[0] == currentBlockIndex) {
	    readPosition[1] += currentOffset;
	}
	return readPosition;
    }

    private long readTerm() {
	if (currentOffset > (currentBlock.length - 8)) {
	    currentOffset = 0;
	    currentBlock = index.getBlock(++currentBlockIndex, context);
	}

	temp[0] = currentOffset;
	long value = Utils.decodeLong2(currentBlock, temp);

	currentOffset = temp[1];
	return value;
    }

    @Override
    public boolean isReady() {
	return true;
    }
}
