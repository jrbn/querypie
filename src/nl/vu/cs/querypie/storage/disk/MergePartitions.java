package nl.vu.cs.querypie.storage.disk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergePartitions implements Iterator<long[]> {

    static final Logger log = LoggerFactory.getLogger(MergePartitions.class);

    long[][] partitionTable;

    public long[][] returnPartitionTable() {
	return partitionTable;
    }

    @SuppressWarnings("unchecked")
    public void init(int myPartition, int nPartitionsPerNode, int nNodes,
	    TripleFile[] files, boolean cacheExists, long[][] firstElements) {
	List<TripleFile>[] lists = new List[nPartitionsPerNode];

	// Start reading the files
	partitionTable = new long[nNodes][3];

	List<Integer> partitionFiles = new ArrayList<Integer>();
	int pIndex = 1;

	for (int i = 0; i < files.length; ++i) {
	    TripleFile file = files[i];
	    String name = file.getName();
	    int partition = Integer.valueOf(name.substring(
		    name.lastIndexOf('-') + 1, name.lastIndexOf('_')));

	    int suffix = Integer.valueOf(name.substring(name.indexOf('_') + 1));

	    if (suffix == 0) {
		if (partition == pIndex * nPartitionsPerNode) {
		    // Sort them and get the minimum
		    long[] min = getMinimum(partitionFiles, files,
			    firstElements);
		    partitionTable[pIndex - 1][0] = min[0];
		    partitionTable[pIndex - 1][1] = min[1];
		    partitionTable[pIndex - 1][2] = min[2];
		    partitionFiles.clear();
		    pIndex++;
		}

		partitionFiles.add(i);
	    }

	    if (!cacheExists
		    && partition >= myPartition * nPartitionsPerNode
		    && (partition < ((myPartition + 1) * nPartitionsPerNode) || myPartition == nNodes - 1)) {
		if (lists[partition - myPartition * nPartitionsPerNode] == null) {
		    lists[partition - myPartition * nPartitionsPerNode] = new ArrayList<TripleFile>();
		}
		lists[partition - myPartition * nPartitionsPerNode].add(file);
	    }
	}

	// Add the last partition
	long[] min = getMinimum(partitionFiles, files, firstElements);
	partitionTable[pIndex - 1][0] = min[0];
	partitionTable[pIndex - 1][1] = min[1];
	partitionTable[pIndex - 1][2] = min[2];

	if (!cacheExists) {
	    sortedList.clear();
	    for (List<TripleFile> list : lists) {
		SortedStream el = new SortedStream();
		el.list = list;
		// Open them
		for (TripleFile file : list) {
		    file.open();
		}
		if (readNextElement(el)) {
		    sortedList.add(el);
		} else {
		    log.warn("The current stream does not contain any element");
		}
	    }
	    minimumValue = null;
	}
    }

    private long[] getMinimum(List<Integer> indices, TripleFile[] files,
	    long[][] firstIndices) {
	sortedList.clear();
	for (int i : indices) {
	    if (firstIndices != null) {
		SortedStream ss = new SortedStream();
		ss.minimum[0] = firstIndices[i][0];
		ss.minimum[1] = firstIndices[i][1];
		ss.minimum[2] = firstIndices[i][2];
		sortedList.add(ss);
	    } else {
		TripleFile f = files[i];
		f.open();
		if (f.next()) {
		    SortedStream ss = new SortedStream();
		    ss.minimum[0] = f.getFirstTerm();
		    ss.minimum[1] = f.getSecondTerm();
		    ss.minimum[2] = f.getThirdTerm();
		    sortedList.add(ss);
		}
		f.close();
	    }
	}

	SortedStream ss = sortedList.pollFirst();
	return ss.minimum;
    }

    class SortedStream {
	long[] minimum = new long[3];
	List<TripleFile> list;
    }

    TreeSet<SortedStream> sortedList = new TreeSet<MergePartitions.SortedStream>(
	    new Comparator<SortedStream>() {
		@Override
		public int compare(SortedStream o1, SortedStream o2) {
		    if (o1.minimum[0] == o2.minimum[0]) {
			if (o1.minimum[1] == o2.minimum[1]) {
			    if (o1.minimum[2] == o2.minimum[2]) {
				return 0;
			    } else if (o1.minimum[2] < o2.minimum[2]) {
				return -1;
			    } else {
				return 1;
			    }
			} else if (o1.minimum[1] < o2.minimum[1]) {
			    return -1;
			} else {
			    return 1;
			}
		    } else if (o1.minimum[0] < o2.minimum[0]) {
			return -1;
		    } else {
			return 1;
		    }
		}
	    });
    SortedStream minimumValue = null;

    @Override
    public boolean hasNext() {
	if (minimumValue != null) {
	    if (readNextElement(minimumValue)) {
		sortedList.add(minimumValue);
	    }
	}

	if (sortedList.size() > 0) {
	    minimumValue = sortedList.pollFirst();
	    return true;
	} else {
	    return false;
	}

    }

    private boolean readNextElement(SortedStream stream) {

	if (stream.list.size() > 0) {
	    TripleFile file = stream.list.get(0);
	    if (file.next()) {
		stream.minimum[0] = file.getFirstTerm();
		stream.minimum[1] = file.getSecondTerm();
		stream.minimum[2] = file.getThirdTerm();
		return true;
	    } else {
		file.close();
		stream.list.remove(0);
		return readNextElement(stream);
	    }
	}

	return false;
    }

    @Override
    public long[] next() {
	return minimumValue.minimum;
    }

    @Override
    public void remove() {
    }

}
