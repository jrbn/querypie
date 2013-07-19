package nl.vu.cs.querypie.storage.disk;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;

public class FirstLayer {
    
    public static final Comparator<String> lbComparator = new Comparator<String>() {
	 @Override
	    public int compare(String arg0, String arg1) {
		if (arg0.length() < arg1.length()) {
		    return -1;
		}
		if (arg0.length() > arg1.length()) {
		    return 1;
		}
		return arg0.compareTo(arg1);
	    }
    };

    // Forget a listblock if the total size of the listblocks exceeds this ...

    public static final long MAX_LISTBLOCKSSIZE_TO_KEEP_CACHE = 64L * 1024 * 1024; // Integers.

    public static final int BLOCK_SIZE_FIRST_ENTRY = 256 * 1024; // 256K entries

    protected byte[] tmpBuffer = new byte[12 * BLOCK_SIZE_FIRST_ENTRY + 4];
    protected IntBuffer intBuffer = ByteBuffer.wrap(tmpBuffer).asIntBuffer();
    
    public HashSet<String> listBlockNameSet = new HashSet<String>();
    public String[] sortedListBlockNames;

    static final Logger log = LoggerFactory.getLogger(FirstLayer.class);

    public class ListBlocks {

	private int[][] list = new int[128][];
	private boolean modified;
	private boolean[] modifiedBlock = new boolean[128];
	public long timestamp;
	int listBlockSize = 0;
	private int[] firstEntries = new int[128];
	private final String name;

	public ListBlocks(int lb, String name) {
	    this(lb, name, BLOCK_SIZE_FIRST_ENTRY);
	}
	
	public ListBlocks(int lb, String name, int sz) {
	    this.name = name;
	    list[0] = new int[1];
	    list[0][0] = 1; // Position of the last block
	    if (lb != -1) {
		list[1] = new int[sz * 3 + 1];
		list[1][0] = 1; // Position you can start writing
	    }
	}

	public int getNumBlocks() {
	    return list[0][0];
	}

	public void addValue(int value, int blockNumber, int offset) {
	    // Get block
	    int posLastBlock = list[0][0];

	    int[] block = list[posLastBlock];
	    int sizeBlock = block[0];

	    if (sizeBlock >= block.length) {
		posLastBlock++;
		if (posLastBlock == list.length) {
		    // Need to create a bigger list!
		    int[][] newList = new int[list.length + list.length / 2][];
		    System.arraycopy(list, 0, newList, 0, list.length);
		    list = newList;
		    int[] entries = new int[list.length];
		    System.arraycopy(firstEntries, 0, entries, 0,
			    firstEntries.length);
		    firstEntries = entries;
		}
		block = new int[BLOCK_SIZE_FIRST_ENTRY * 3 + 1];
		sizeBlock = 1;

		// Update the list
		list[posLastBlock] = block;
		list[0][0] = posLastBlock;
	    }
	    if (sizeBlock == 1) {
		firstEntries[posLastBlock] = value;
	    }

	    block[sizeBlock] = value;
	    block[sizeBlock + 1] = blockNumber;
	    block[sizeBlock + 2] = offset;
	    block[0] = sizeBlock + 3;
	    listBlockSize += 3;
	}
	
	public int getListBlockNo(int value, ActionContext context) throws Exception {
	    int blockBegin = 1;
	    int blockEnd = list[0][0];

	    while (blockBegin < blockEnd) {
		int currentBlock = (int) Math
			.ceil((double) (blockBegin + blockEnd) / 2);
		int firstValue;
		if (firstEntries == null) {
		    firstValue = list[currentBlock][1];
		} else {
		    firstValue = firstEntries[currentBlock];
		}
		if (value < firstValue) {
		    blockEnd = currentBlock - 1;
		} else {
		    blockBegin = currentBlock;
		}
	    }

	    // Now I have identified the good block.
	    if (list[blockBegin] == null) {
		// context.incrCounter("ListBlocks cache miss", 1);
		list[blockBegin] = readBlock(name, blockBegin, context);
		listBlockSize += list[blockBegin][0];
	    }
	    return blockBegin;
	}
	
	public int[] getListBlock(int blockno) throws Exception {
	    if (list[blockno] == null) {
		list[blockno] = readBlock(name, blockno, null);
		listBlockSize += list[blockno][0];
	    }
	    return list[blockno];
	}

	public boolean getValue(int value, int[] coordinates,
		ActionContext context) throws Exception {
	    
	    int blockNo = getListBlockNo(value, context);

	    int[] block = list[blockNo];
	    int begin = 1;
	    int end = block[0];
	    while (end - begin > 3) {
		int posMiddle = (((end - begin) / 3) / 2) * 3 + begin;
		if (value < block[posMiddle]) {
		    end = posMiddle;
		} else if (value > block[posMiddle]) {
		    begin = posMiddle;
		} else {
		    begin = end = posMiddle;
		}
	    }
	    
	    if (log.isDebugEnabled()) {
	        log.debug("looking for value " + value + ", found " + block[begin]
	                + ", position = " + block[begin+1] + " " + block[begin+2] + " at index " + begin);
	    }

	    if (block[begin] == value) {
		// coordinates[2] will get an estimate of the size, or
		// Integer.MAX_VALUE.
		coordinates[0] = block[begin + 1];
		coordinates[1] = block[begin + 2];
		if (coordinates.length > 2) {
		    coordinates[2] = Integer.MAX_VALUE;
		    if (begin + 5 < block.length) {
			// Next entry is also in this block.
			int nblocks = block[begin + 4] - coordinates[0];
			if (log.isDebugEnabled()) {
			    log.debug("nblocks = " + nblocks);
			}
			if (nblocks < 100) {
			    coordinates[2] = block[begin + 5] - coordinates[1];
			    // The next size estimate may be a bit off when dealing with an
			    // incrementally created cache.
			    coordinates[2] += (block[begin + 4] - coordinates[0])
				    * Index.BLOCK_SIZE;
			}
		    } else if (++blockNo <= list[0][0]) {
			if (list[blockNo] == null) {
			    // context.incrCounter("ListBlocks cache miss", 1);
			    list[blockNo] = readBlock(name, blockNo,
				    context);
			    listBlockSize += list[blockNo][0];
			}
			block = list[blockNo];
			int nblocks = block[2] - coordinates[0];
			if (log.isDebugEnabled()) {
			    log.debug("nblocks = " + nblocks);
			}
			if (nblocks < 100) {
			    coordinates[2] = block[3] - coordinates[1];
			    // The next size estimate may be a bit off when dealing with an
                            // incrementally created cache.
			    coordinates[2] += nblocks * Index.BLOCK_SIZE;
			}
		    } else {
			if (log.isDebugEnabled()) {
			    log.debug("Pity, hit the last entry of a listblock");
			    log.debug("begin = " + begin + ", block.length = "
				    + block.length);
			    log.debug("blockNo = " + blockNo
				    + ", list[0][0] = " + list[0][0]);
			}
		    }

		}
		return true;
	    }

	    return false;
	}

	public void writeTo(DataOutputStream stream, boolean writeBlocks) throws Exception {
	    int size = list[0][0];
	    stream.writeInt(size); // Number of blocks
	    intBuffer.rewind();
	    intBuffer.put(firstEntries, 1, size);
	    stream.write(tmpBuffer, 0, size * 4);

	    if (writeBlocks) {
		for (int i = 1; i <= size; ++i) {
		    writeBlock(list[i], i, name);
		}
	    }
	}

	public void readFrom(DataInputStream stream) throws IOException {
	    int size = stream.readInt();
	    list = new int[size + 1][];
	    list[0] = new int[1];
	    list[0][0] = size;
	    listBlockSize = 0;
	    firstEntries = new int[size + 1];
	    modifiedBlock = new boolean[size+1];
	    stream.readFully(tmpBuffer, 0, size * 4);
	    intBuffer.rewind();
	    intBuffer.get(firstEntries, 1, size);
	}
    }

    final Index index;

    /**
     * @param index
     */
    FirstLayer(Index index) {
	this.index = index;
    }
    
    public void setListBlockNames(String[] names) {
	listBlockNameSet.clear();
	sortedListBlockNames = new String[names.length];
	System.arraycopy(names, 0, sortedListBlockNames, 0, names.length);
	Arrays.sort(sortedListBlockNames, lbComparator);
	for (String name : names) {
	    listBlockNameSet.add(name);
	}	
    }

    long timer = Long.MIN_VALUE; // counter used for maintaining oldest
    // access

    private ListBlocks[] map = new ListBlocks[512];
    private int lastFirstPart = -1;
    private long totalListBlocksSize = 0;

    // private int me = -1;

    public void addValue(long resource, int block, int offset) throws Exception {
	int firstPart = ((int) (resource >> 40) & 0xFFFFFF);
	int secondPart = (int) (resource & 0xFFFFFFFFFFl);

	// Increase map size if needed.
	if (firstPart >= map.length) {
	    int len = 2 * map.length;
	    while (firstPart >= len) {
		len *= 2;
	    }
	    ListBlocks[] newMap = new ListBlocks[len];
	    System.arraycopy(map, 0, newMap, 0, map.length);
	    map = newMap;
	}

	if (firstPart != lastFirstPart) {
	    // Sanity check, to make sure that ListBlocks are filled
	    // one at a time.
	    if (map[firstPart] != null) {
		log.error("Something wrong!");
	    }

	    // Flush the previous buffer.
	    if (lastFirstPart >= 0) {
		try {
		    writeListBlockToFile(map[lastFirstPart], lastFirstPart, true);
		} catch (IOException e) {
		    log.error("Got exception while writing ListBlock", e);
		}
		map[lastFirstPart] = null;
	    }
	    
	    String name = "lb-" + firstPart;
	    listBlockNameSet.add(name);

	    // Allocate the new buffer.
	    map[firstPart] = new ListBlocks(firstPart, "lb-" + firstPart);
	    lastFirstPart = firstPart;

	    // Give it a valid timestamp.
	    map[firstPart].timestamp = timer++;
	}
	map[firstPart].addValue(secondPart, block, offset);
    }

    int[] readBlock(String name, int blockNo, ActionContext context)
	    throws Exception {
	getIOLock();
	File f = index.getCacheFile(name + "/" + blockNo);
	int[] result = null;
 	try {
	    if (log.isDebugEnabled()) {
		log.debug("Reading block " + blockNo + " of " + name);
	    }
	    long time = System.currentTimeMillis();
	    InputStream reader = index.getFilesInterface().createInputStream(f);
	    DataInputStream stream = new DataInputStream(
		    index.codec.createInputStream(reader));
	            // reader);
	    int sz = stream.readInt();
	    result = new int[sz];
	    result[0] = sz;
	    synchronized (tmpBuffer) {
		if (tmpBuffer.length < (sz - 1) * 4) {
		    tmpBuffer = new byte[(sz - 1) * 4];
		    intBuffer = ByteBuffer.wrap(tmpBuffer).asIntBuffer();
		}

		stream.readFully(tmpBuffer, 0, (sz - 1) * 4);
		intBuffer.rewind();
		intBuffer.get(result, 1, sz - 1);
	    }
	    stream.close();
	    reader.close();

            /*
            if (me == -1) {
                me = context.getNetworkLayer().getMyPartition();
            }
            context.incrCounter("Node " + me + ", time spent reading lb from disk",
		    System.currentTimeMillis() - time);
	    context.incrCounter("Node " + me + ", lb bytes read from disk", sz * 4);
            */
	    if (context != null) {
		context.incrCounter("Time spent reading lb from disk",
			System.currentTimeMillis() - time);
		context.incrCounter("Lb bytes read from disk", sz * 4);
	    }
	    totalListBlocksSize += sz;
	    if (totalListBlocksSize > MAX_LISTBLOCKSSIZE_TO_KEEP_CACHE) {
		// We already have the maximum number of
		// ListBlocks in core, so we have
		// to delete one.
		removeOldestBlockInCache();
	    }
	    if (log.isDebugEnabled()) {
		log.debug("After readBlock: totalListBlocksSize = "
			+ totalListBlocksSize);
	    }
	} catch (IOException e) {
	    log.error("Could not read block " + f.getAbsolutePath(), e);
	} finally {
	    releaseIOLock();
	}
	return result;
    }

    void writeBlock(int[] block, int blockNo, String name) throws Exception {
	String directory = index.cacheDir;
	File f = index.getFilesInterface().createFile(
		directory + File.separator + name + File.separator + blockNo);
	if (f.exists()) {
            File savedDir = index.getFilesInterface().createFile(directory + File.separator + "saved-" + name);
            savedDir.mkdirs();
            File savedF = index.getFilesInterface().createFile(directory + File.separator + "saved-" + name + File.separator + blockNo);
            int i = 1;
            while (savedF.exists()) {
                savedF = index.getFilesInterface().createFile(directory + File.separator + "saved-" + name + File.separator + blockNo
                        + "-" + i++);
            }
            f.renameTo(savedF);
        }
	OutputStream writer = index.getFilesInterface().createOutputStream(f);
	DataOutputStream stream = new DataOutputStream(
		index.codec.createOutputStream(writer));
	        // writer);

	int sz = block[0];
	if (tmpBuffer.length < sz * 4) {
	    tmpBuffer = new byte[sz * 4];
	    intBuffer = ByteBuffer.wrap(tmpBuffer).asIntBuffer();
	}

	intBuffer.rewind();
	intBuffer.put(block, 0, sz);
	stream.write(tmpBuffer, 0, sz * 4);
	stream.close();
	writer.close();
	trimFile(f);
    }

    private void removeOldestBlockInCache() throws Exception {
	long minimum = Long.MAX_VALUE;
	int e = -1;
	// Find the "oldest" listBlock. Sequential search over the
	// elements,
	// but I guess the time this takes is negligible with respect to
	// the time
	// it takes to read another ListBlock.
	for (int i = 0; i < map.length; i++) {
	    if (map[i] == null || map[i].listBlockSize == 0) {
		continue;
	    }
	    if (map[i].timestamp < minimum) {
		e = i;
		minimum = map[i].timestamp;
	    }
	}
	if (e >= 0) {
	    if (log.isDebugEnabled()) {
		log.debug("ListBlocks cache full(totalListBlocksSize = "
			+ totalListBlocksSize + ", forgetting blocklist " + e);
	    }
	    if (map[e].modified) {
		flush(map[e], e);
	    }
	    totalListBlocksSize -= map[e].listBlockSize;
	    map[e].listBlockSize = 0;
	    for (int i = 1; i < map[e].list.length; i++) {
		map[e].list[i] = null;
	    }
	}
    }
    
    public void flush(ListBlocks  b, int no) throws Exception {
	writeListBlockToFile(b, no, false);
	b.modified = false;
	for (int i = 1; i < b.list.length; i++) {
	    if (b.modifiedBlock[i]) {
		writeBlock(b.list[i], i, b.name);
		b.modifiedBlock[i] = false;
	    }
	}
    }

    public ListBlocks getBlock(long entry, ActionContext context) throws Exception {

	int firstPart = ((int) (entry >> 40) & 0xFFFFFF);
	
	return getBlock(firstPart, context);
    }
    
    public ListBlocks getBlock(int firstPart, ActionContext context) throws Exception {

	// Increase map size if needed.
	if (firstPart >= map.length) {
	    int len = 2 * map.length;
	    while (firstPart >= len) {
		len *= 2;
	    }
	    ListBlocks[] newMap = new ListBlocks[len];
	    System.arraycopy(map, 0, newMap, 0, map.length);
	    map = newMap;
	}
	
	// Do we have the corresponding ListBlock in core?
	if (map[firstPart] == null) {
	    // No, we don't, so we have to read it.
	    // Read the corresponding ListBlock.
	    long time = System.currentTimeMillis();
	    map[firstPart] = readListBlockFromFile(firstPart);
	    if (map[firstPart] == null) {
		return null;
	    }
	    if (context != null) {
		context.incrCounter("Time spent reading lb from disk",
			System.currentTimeMillis() - time);
		context.incrCounter("Lb bytes read from disk",
			map[firstPart].listBlockSize * 4);
	    }
	    // context.incrCounter("ListBlocks cache first entries miss", 1);
	} else {
	    // context.incrCounter("ListBlocks cache first entries hit", 1);
	}
	map[firstPart].timestamp = timer++;
	if (log.isDebugEnabled()) {
	    log.debug("Found LB for firstPart " + firstPart);
	}
	return map[firstPart];
    }

    // Note: coordinates[2] will get an estimate of the size, or
    // Integer.MAX_VALUE.
    public boolean getInf(long entry, int[] coordinates, ActionContext context)
	    throws Exception {
	
	ListBlocks e = getBlock(entry, context);
	if (e == null) {
	    return false;
	}

	int secondPart = (int) (entry & 0xFFFFFFFFFFl);
	e.timestamp = timer++;
	return e.getValue(secondPart, coordinates, context);
    }

    // Reads a ListBlocks object from the specified file in the specified
    // directory.
    ListBlocks readListBlockFromFile(int no) throws Exception {
	String dir = index.cacheDir;
	String name = "lb-" + no;
	if (! listBlockNameSet.contains(name)) {
	    return null;
	}
	getIOLock();
	File file = index.getCacheFile(name + "/index");
	try {
	    InputStream reader = index.getFilesInterface().createInputStream(
		    file);
	    DataInputStream stream = new DataInputStream(
		    this.index.codec.createInputStream(reader));
	            // reader);
	    ListBlocks block = new ListBlocks(no, name, 1);
	    block.readFrom(stream);
	    stream.close();
	    reader.close();
	    if (log.isDebugEnabled()) {
		log.debug("done reading index of ListBlock " + dir + "/" + name);
	    }
	    return block;
	} catch (Exception e) {
	    log.error("Failed reading the file: " + file, e);
	    throw e;
	} finally {
	    releaseIOLock();
	}
    }

    // Writes the specified ListBlocks object to the specified file in the
    // specified directory.
    void writeListBlockToFile(ListBlocks b, int no, boolean writeBlocks) throws Exception {
	String name = "lb-" + no;
	File file = index.getFilesInterface().createFile(
		index.cacheDir + "/" + name);
        if (! file.mkdirs() && (! file.exists() || ! file.isDirectory())) {
	    log.warn(index.cacheDir + File.separator + name
		    + ": could not create");
	    /*
	     * Don't make this a fatal error ... --Ceriel throw new
	     * IOException("Could not create" + index.cacheDir + File.separator
	     * + name);
	     */
	}
	file = index.getFilesInterface().createFile(
		index.cacheDir + "/" + name + "/index");
	if (log.isDebugEnabled()) {
	    log.debug("write index of ListBlock " + index.cacheDir + "/" + name);
	}
	if (file.exists()) {
            File savedDir = index.getFilesInterface().createFile(index.cacheDir + "/" + "saved-" + name);
            savedDir.mkdirs();
            File savedF = index.getFilesInterface().createFile(index.cacheDir + "/" + "saved-" + name + "/index");
            int i = 1;
            while (savedF.exists()) {
                savedF = index.getFilesInterface().createFile(index.cacheDir + "/" + "saved-" + name + "/index-" + i++);
            }
            file.renameTo(savedF);
        }
	OutputStream writer = index.getFilesInterface()
		.createOutputStream(file);
	DataOutputStream stream = new DataOutputStream(
		this.index.codec.createOutputStream(writer));
	        // writer);
	b.writeTo(stream, writeBlocks);
	stream.flush();
	stream.close();
	writer.flush();
	writer.close();
	trimFile(file);
    }

    // Copy to get the number of blocks down. Without this, a file can be .5
    // Mbyte but
    // have 1Mbyte worth of blocks.
    void trimFile(File f) {
	try {
	    long length = f.length();
	    RandomAccessFile raf = index.getFilesInterface()
		    .createRandomAccessFile(f);
	    raf.seek(length);
	    raf.write(' ');
	    raf.setLength(length);
	    raf.close();
	} catch (Exception ex) {
	    log.error("Could not trim " + f.getPath());
	}
    }

    void writeTo() throws Exception {
	// Write the blocks in separate files
	for (int i = 0; i < map.length; ++i) {
	    if (map[i] != null) {
		writeListBlockToFile(map[i], i, true);
	    }
	}
    }

    private static int IOThreads;
    private static final int MAX_IOTHREADS = 1;

    static synchronized void getIOLock() {
	while (IOThreads >= MAX_IOTHREADS) {
	    try {
		FirstLayer.class.wait();
	    } catch (InterruptedException e) {
		// ignore
	    }
	}
	IOThreads++;
    }

    static synchronized void releaseIOLock() {
	IOThreads--;
	if (IOThreads < MAX_IOTHREADS) {
	    FirstLayer.class.notify();
	}
    }

    public void shiftListBlocks(long subject, int currentBlockIndex, int len) throws Exception {
	int firstPart = ((int) (subject >> 40) & 0xFFFFFF);
	int secondPart = (int) (subject & 0xFFFFFFFFFFl);
	ListBlocks b = getBlock(firstPart, null);
	int blockNo = b.getListBlockNo(secondPart, null);
	int[] block = b.getListBlock(blockNo);
	int begin = 1;
	int end = block[0];
	while (end - begin > 3) {
	    int posMiddle = (((end - begin) / 3) / 2) * 3 + begin;
	    if (secondPart < block[posMiddle]) {
		end = posMiddle;
	    } else if (secondPart > block[posMiddle]) {
		begin = posMiddle;
	    } else {
		begin = end = posMiddle;
	    }
	}
	if (log.isDebugEnabled()) {
	    log.debug("shiftListBlocks, secondPart = " + secondPart
	            + ", now at index " + begin + ", value = " +  block[begin]
	                    + ", len = " + len);
	}
	for (int i = begin+3; i < block[0]; i += 3) {
	    if (block[i+1] < currentBlockIndex) {
		continue;
	    }
	    if (block[i+1] > currentBlockIndex) {
	        if (log.isDebugEnabled()) {
	            log.debug("Found block number " + block[i+1] + ", was shifting for block " + currentBlockIndex);
	        }
		return;
	    }

	    block[i+2] += len;
	    // index.checkPosition(block[i+1], block[i+2]);
	    if (log.isDebugEnabled()) {
	        log.debug("shifting " + block[i] + " to " + block[i+2]);
	    }

	    b.modified = true;
	    b.modifiedBlock[blockNo] = true;
	}
	for (blockNo = blockNo+1; blockNo <= b.getNumBlocks(); blockNo++) {
	    block = b.getListBlock(blockNo);
	    for (int i = 1; i < block[0]; i += 3) {
		if (block[i+1] > currentBlockIndex) {
		    return;
		}
		block[i+2] += len;
		// index.checkPosition(block[i+1], block[i+2]);
		if (log.isDebugEnabled()) {
		    log.debug("Shifting " + block[i] + " to " + block[i+2]);
		}
		b.modified = true;
		b.modifiedBlock[blockNo] = true;
	    }
	}
	// If we get here, we have to shift next listBlocks as well.
	String name = "lb-" + firstPart;
	int firstPartIndex = Arrays.binarySearch(sortedListBlockNames, name, lbComparator);
	for (int i = firstPartIndex + 1; i < sortedListBlockNames.length; i++) {
	    if (log.isDebugEnabled()) {
	        log.debug("Also shifting listBlock " + sortedListBlockNames[i]);
	    }
	    firstPart = Integer.valueOf(sortedListBlockNames[i].substring(3));
	    b = getBlock(firstPart, null);
	    for (int j = 1; j <= b.getNumBlocks(); j++) {
		block = b.getListBlock(j);
		for (int k = 1; k < block[0]; k += 3) {
		    if (block[k+1] > currentBlockIndex) {
			return;
		    }
		    block[k+2] += len;
		    // index.checkPosition(block[k+1], block[k+2]);
		    if (log.isDebugEnabled()) {
		        log.debug("Shifting " + block[k] + " to " + block[k+2]);
		    }
		    b.modified = true;
		    b.modifiedBlock[j] = true;
		}
	    }
	}
    }
    
    private void getPosFromNextBlock(int i, int[] position) throws Exception {
	if (i < sortedListBlockNames.length) {
	    int firstPart = Integer.valueOf(sortedListBlockNames[i].substring(3));
	    ListBlocks b = getBlock(firstPart, null);
	    int[] block = b.getListBlock(1);
	    position[0] = block[2];
	    position[1] = block[3];
	    if (log.isDebugEnabled()) {
                log.debug("Insertion point for subject: " + position[0] + " " + position[1]);
            }
	    return;
	}
	// Here we need to find the very end 
	position[0] = index.lastBlockNo;
	byte[] bl = index.getLastBlock();
	i = bl.length-1;
	while (bl[i] != Index.FLAG_END_SEQUENCE) {
	    i--;
	}
	position[1] = i+1;
	if (log.isDebugEnabled()) {
            log.debug("Insertion point for subject: " + position[0] + " " + position[1]);
        }
    }
    
    public boolean getSubjectPosition(long subject, int[] position) throws Exception {
	int firstPart = ((int) (subject >> 40) & 0xFFFFFF);
	int secondPart = (int) (subject & 0xFFFFFFFFFFl);
	ListBlocks b = getBlock(firstPart, null);
	if (b == null) {
	    if (log.isDebugEnabled()) {
	        log.debug("getSubjectPosition: LB " + firstPart + " not present.");
	    }
	    String name = "lb-" + firstPart;
	    int i = Arrays.binarySearch(sortedListBlockNames, name, lbComparator);
	    // It is not present, so it will return -insertionPosition-1.
	    i = -(i + 1);
	    if (log.isDebugEnabled()) {
	        if (i < sortedListBlockNames.length) {
	            log.debug("Next present name = " + sortedListBlockNames[i]);
	        }
	        if (i > 0) {
	            log.debug("Previous present name = " + sortedListBlockNames[i-1]);
	        }
	    }
	    // Now get the first position of the next listblock.
	    getPosFromNextBlock(i, position);
	    return false;
	}
	int listBlockNo = b.getListBlockNo(secondPart, null);
        int[] listBlock = b.getListBlock(listBlockNo);

        // Now, find the subject in the listblock.
        int begin = 1;
        int end = listBlock[0];
        if (listBlock[end-3] < secondPart) {
            // Get insertion point from next listblock.
            if (listBlockNo+1 <= b.getNumBlocks()) {
        	listBlockNo++;
        	listBlock = b.getListBlock(listBlockNo);
        	position[0] = listBlock[2];
        	position[1] = listBlock[3];
        	if (log.isDebugEnabled()) {
        	    log.debug("Insertion point for subject: " + position[0] + " " + position[1]);
        	}
        	return false;
            }
            // Does not exist.
            String name = "lb-" + firstPart;
            int i = Arrays.binarySearch(sortedListBlockNames, name, lbComparator);
            getPosFromNextBlock(i+1, position);
            return false;
        }
        while (end - begin > 3) {
            int posMiddle = (((end - begin) / 3) / 2) * 3 + begin;
            if (secondPart < listBlock[posMiddle]) {
        	end = posMiddle;
            } else if (secondPart > listBlock[posMiddle]) {
        	begin = posMiddle;
            } else {
        	begin = end = posMiddle;
            }
        }
        if (secondPart > listBlock[begin]) {
            begin += 3;
        }
        // Now, secondPart should be <= listBlock[begin].
        if (log.isDebugEnabled()) {
            if (secondPart > listBlock[begin]) {
                log.error("Oops, secondPart = " + secondPart + ", listBlock[begin] = " + listBlock[begin]);
            }
        }
        
        position[0] = listBlock[begin+1];
        position[1] = listBlock[begin+2];
        boolean present = secondPart == listBlock[begin];
        if (log.isDebugEnabled()) {
            log.debug("Subject " + (present ? "" : "not ")
                    + "present, position = " + position[0] + " " + position[1]);              
        }
        return present;
    }

    public void updateLB(long subject, int[] pos) throws Exception {
	int firstPart = ((int) (subject >> 40) & 0xFFFFFF);
	int secondPart = (int) (subject & 0xFFFFFFFFFFl);
	ListBlocks b = getBlock(firstPart, null);
	if (b == null) {
	    String name = "lb-" + firstPart;
	    listBlockNameSet.add(name);
	    setListBlockNames(listBlockNameSet.toArray(new String[listBlockNameSet.size()]));
	    b = new ListBlocks(firstPart, name, 1);
	    b.modified = true;
	    b.modifiedBlock[1] = true;
	    b.firstEntries = new int[2];
	    b.firstEntries[1] = secondPart;
	    b.list[1][1] = secondPart;
	    b.list[1][2] = pos[0];
	    b.list[1][3] = pos[1];
	    b.list[1][0] = 4;
	    // Increase map size if needed.
	    if (firstPart >= map.length) {
		int len = 2 * map.length;
		while (firstPart >= len) {
		    len *= 2;
		}
		ListBlocks[] newMap = new ListBlocks[len];
		System.arraycopy(map, 0, newMap, 0, map.length);
		map = newMap;
	    }
	    map[firstPart] = b;
	} else {
	    int listBlockNo = b.getListBlockNo(secondPart, null);
	    int[] listBlock = b.getListBlock(listBlockNo);

	    // Now, find the subject in the listblock.
	    int begin = 1;
	    int end = listBlock[0];
	    if (listBlock[end-3] < secondPart) {
		// Append to this block.
		int[] newBlock = new int[listBlock.length+3];
		System.arraycopy(listBlock, 0, newBlock, 0, listBlock.length);
		newBlock[0] = end + 3;
		newBlock[end] = secondPart;
		newBlock[end+1] = pos[0];
		newBlock[end+2] = pos[1];
		b.list[listBlockNo] = newBlock;
	    } else {
		while (end - begin > 3) {
		    int posMiddle = (((end - begin) / 3) / 2) * 3 + begin;
		    if (secondPart < listBlock[posMiddle]) {
			end = posMiddle;
		    } else if (secondPart > listBlock[posMiddle]) {
			begin = posMiddle;
		    } else {
			begin = end = posMiddle;
		    }
		}
		if (secondPart > listBlock[begin]) {
		    begin += 3;
		}
		int[] newBlock = new int[listBlock.length+3];
		System.arraycopy(listBlock, 0, newBlock, 0, begin);
		System.arraycopy(listBlock, begin, newBlock, begin+3, listBlock[0] - begin);
		newBlock[0] = listBlock[0] + 3;
		newBlock[begin] = secondPart;
		newBlock[begin+1] = pos[0];
		newBlock[begin+2] = pos[1];
		b.list[listBlockNo] = newBlock;
		if (begin == 1) {
		    b.firstEntries[listBlockNo] = secondPart;
		}
	    }
	    b.modified = true;
	    b.modifiedBlock[listBlockNo] = true;
	}
	map[firstPart].timestamp = timer++;
    }

    public void flushListBlocks() throws Exception {
        for (int i = 0; i < map.length; i++) {
            ListBlocks b = map[i];
	    if (b != null && b.modified) {
	        writeListBlockToFile(b, i, false);
	        b.modified = false;
	        for (int j = 1; j <= b.list[0][0]; ++j) {
	            if (b.modifiedBlock[j]) {
	                writeBlock(b.list[j], j, b.name);
	                b.modifiedBlock[j] = false;
	            }
                }
	    }
	}
	
    }
}
