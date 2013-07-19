package nl.vu.cs.querypie.storage.disk;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.vu.cs.querypie.utils.Utils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.utils.LongMap;

import com.hadoop.compression.lzo.LzoCodec;

public class Index {

    public static final Pattern p = Pattern.compile("[0-9]+");

    static final Logger log = LoggerFactory.getLogger(Index.class);

    public static final int BLOCK_SIZE = 1 * 1024 * 1024; // 1 MB.

    public static final byte FLAG_NEXT_OBJECT = 1;
    public static final byte FLAG_NEXT_PREDICATE = 2;
    public static final byte FLAG_END_SEQUENCE = 3;

    private static class BlocksCache {
	private final LongMap<Block> fromCacheBlocks = new LongMap<Block>();
	private Block head, tail;
	public final long MAX_SIZE;

	public BlocksCache(long maxSize) {
	    this.MAX_SIZE = maxSize;
	}

	protected synchronized Block removeOldestBlockInCache() {
	    Block b = head;
	    if (b != null) {
		head = b.next;
		if (head == null) {
		    head = tail = null;
		} else {
		    head.prev = null;
		}
	    }

	    if (log.isDebugEnabled()) {
		log.debug("removing block " + b.index + " from cache");
	    }
	    fromCacheBlocks.remove(b.index);
	    return b;
	}

	protected void addBlock(Block b) {
	    fromCacheBlocks.put(b.index, b);
	    if (tail == null) {
		head = tail = b;
	    } else {
		tail.next = b;
		b.prev = tail;
		tail = b;
	    }
	}

	public int size() {
	    return fromCacheBlocks.size();
	}

	public Block get(long key) {
	    return fromCacheBlocks.get(key);
	}

	public void moveToEnd(Block block) {
	    if (block == tail) {
		// nothing
	    } else {
		// Remove block from current spot
		block.next.prev = block.prev;
		if (block.prev != null) {
		    block.prev.next = block.next;
		} else {
		    head = block.next;
		}
		// and append it to the end
		tail.next = block;
		block.prev = tail;
		block.next = null;
		tail = block;
	    }
	}

	public Block removeFromHead() {
	    Block b = head;
	    if (head != null) {
		fromCacheBlocks.remove(head.index);
		head = head.next;
		if (head != null) {
		    head.prev = null;
		} else {
		    tail = null;
		}
	    }
	    return b;
	}

    }

    public static final long MAX_BUFFERS_TO_KEEP_CACHE = (4000L * 1024 * 1024)
	    / BLOCK_SIZE;

    private static final BlocksCache globalCache = new BlocksCache(
	    MAX_BUFFERS_TO_KEEP_CACHE);

    // For cache-updating we use a local cache, for queries we use a global
    // cache.
    public BlocksCache blocksCache;

    // public final LongMap<Block> fromCacheBlocks = new LongMap<Block>();
    // protected Block head, tail; // for blocklist, from old to new.

    protected byte[] currentBlock = null;
    protected int offset = 0;
    protected long entry1 = -1;
    protected long entry2 = -1;

    protected int blockPreviousentry = -1;
    protected int positionPreviousEntry = -1;

    protected FirstLayer s1;
    protected int usedBlocks;

    // int me = -1;

    public LzoCodec codec = new LzoCodec();

    public String globalCacheDir = null;

    public String cacheDir = null;

    static ArrayList<String> indices = new ArrayList<String>();

    private final String name;

    protected long myId;

    private static long idCounter = 0;

    private boolean disableSubjectCache = false;

    public static class Block {

	public byte[] block;
	public long index;
	public Block next;
	public Block prev;
	public boolean marked;
    }

    FilesInterface fi;

    public int[] positions;

    public void disableSubjectCache() {
	disableSubjectCache = true;
    }

    public void setFilesInterface(FilesInterface fi) {
	this.fi = fi;
    }

    public FilesInterface getFilesInterface() {
	return fi;
    }

    public Index(String name, int[] positions, boolean useGlobalCache) {
	currentBlock = new byte[Index.BLOCK_SIZE];
	usedBlocks = 1;
	this.positions = positions;
	if (useGlobalCache) {
	    blocksCache = globalCache;
	} else {
	    blocksCache = new BlocksCache(MAX_BUFFERS_TO_KEEP_CACHE / 6);
	}

	codec.setConf(new Configuration());
	s1 = new FirstLayer(this);
	this.name = name;
	synchronized (this.getClass()) {
	    myId = (idCounter << 32);
	    idCounter++;
	}
    }

    public String getName() {
	return name;
    }

    public int getId() {
	return (int) (myId >> 32);
    }

    public void writeElement(long entry1, long entry2, long entry3)
	    throws Exception {

	if (this.entry1 != entry1) {
	    // Write double separator
	    if (this.entry1 != -1) {
		writeSeparator(FLAG_END_SEQUENCE);
	    }

	    s1.addValue(entry1, usedBlocks - 1, offset);
	    this.entry1 = entry1;
	    this.entry2 = -1;

	    // Set the previous predicate location to MIN_VALUE
	    if (blockPreviousentry != -1) {
		blockPreviousentry = -1;
		positionPreviousEntry = -1;
	    }
	}

	// Write the predicate
	if (this.entry2 != entry2) {
	    if (this.entry2 != -1) {
		writeSeparator(FLAG_NEXT_PREDICATE);
	    }

	    // At the previous location write the offset to this location.
	    if (blockPreviousentry != -1) {
		byte[] b = currentBlock;
		if (blockPreviousentry != usedBlocks - 1) {
		    b = getBlock(blockPreviousentry, false, null).block;
		}
		Utils.encodeInt(b, positionPreviousEntry, usedBlocks - 1);
		// Within same block, encoding is offset from current position,
		// in another block, it is
		// offset from begin of block.
		if (blockPreviousentry == usedBlocks - 1) {
		    Utils.encodeInt(b, positionPreviousEntry + 4, offset
			    - (positionPreviousEntry + 8));
		} else {
		    Utils.encodeInt(b, positionPreviousEntry + 4, offset);
		}
	    }

	    writeTerm(entry2);

	    // Space for position of the next entry2.
	    writePosition(Integer.MIN_VALUE, Integer.MIN_VALUE);
	    blockPreviousentry = usedBlocks - 1;
	    positionPreviousEntry = offset - 8;
	    this.entry2 = entry2;

	    // Write the object without flag
	    writeTerm(entry3);
	} else {
	    writeSeparator(FLAG_NEXT_OBJECT);
	    writeTerm(entry3);
	}
    }

    protected void writeSeparator(byte n) {
	if (offset > BLOCK_SIZE - 1) {
	    cacheBlockForWriting(false, usedBlocks - 1, currentBlock);
	    offset = 0;
	    currentBlock = new byte[BLOCK_SIZE];
	    usedBlocks++;
	}
	currentBlock[offset++] = n;
    }

    private void writePosition(int block, int off) {
	if (offset > (BLOCK_SIZE - 8)) {
	    cacheBlockForWriting(false, usedBlocks - 1, currentBlock);
	    offset = 0;
	    currentBlock = new byte[BLOCK_SIZE];
	    usedBlocks++;
	}

	Utils.encodeInt(currentBlock, offset, block);
	if (block == usedBlocks - 1) {
	    Utils.encodeInt(currentBlock, offset + 4, off - (offset + 8));
	} else {
	    Utils.encodeInt(currentBlock, offset + 4, off);
	}
	offset += 8;
    }

    private void writeTerm(long value) {
	// Write the object
	if (offset > (BLOCK_SIZE - 8)) {
	    cacheBlockForWriting(false, usedBlocks - 1, currentBlock);
	    offset = 0;
	    currentBlock = new byte[BLOCK_SIZE];
	    usedBlocks++;
	}

	offset = Utils.encodeLong2(currentBlock, offset, value);
    }

    private void writeBlockToDisk(Block block) {
	try {
	    String cacheDir = cacheDirs[(int) (block.index >> 32)];
	    int index = (int) (block.index & 0xFFFFFFFFL);
	    File f = fi.createFile(cacheDir + "/" + (index / 1000));
	    f.mkdirs();
	    f = fi.createFile(cacheDir + "/" + (index / 1000) + "/" + index);
	    if (f.exists()) {
		File savedDir = fi.createFile(cacheDir + "/saved_"
			+ (index / 1000));
		savedDir.mkdirs();
		File savedF = fi.createFile(cacheDir + "/saved_"
			+ (index / 1000) + "/" + index);
		int i = 1;
		while (savedF.exists()) {
		    savedF = fi.createFile(cacheDir + "/saved_"
			    + (index / 1000) + "/" + index + "-" + i++);
		}
		f.renameTo(savedF);
	    }

	    OutputStream fout = fi.createOutputStream(f);

	    OutputStream stream = codec.createOutputStream(fout);
	    DataOutputStream dout = new DataOutputStream(stream);
	    // DataOutputStream dout = new DataOutputStream(fout);

	    dout.writeInt(block.block.length);
	    dout.write(block.block);

	    dout.close();
	    fout.close();
	    s1.trimFile(f);
	    if (log.isDebugEnabled()) {
		log.debug("Wrote file " + f.getPath());
	    }
	} catch (Exception e) {
	    log.error("Error writing block", e);
	}
    }

    private void cacheBlockForWriting(boolean flush, int index, byte[] block) {
	Block b = new Block();
	b.block = block;
	b.index = myId + index;
	synchronized (blocksCache) {
	    blocksCache.addBlock(b);
	    if (log.isDebugEnabled()) {
		String name = cacheDir + "/" + (index / 1000) + "/" + index;
		log.debug("Adding block " + name + " to cache");
	    }
	    if (blocksCache.size() >= MAX_BUFFERS_TO_KEEP_CACHE || flush) {
		if (!flush) {
		    b = blocksCache.removeOldestBlockInCache();
		    writeBlockToDisk(b);
		} else {
		    b = blocksCache.removeFromHead();
		    while (b != null) {
			writeBlockToDisk(b);
			b = blocksCache.removeFromHead();
		    }
		}
	    }
	}
    }

    public byte[] getBlock(int index, ActionContext context) {
	return getBlock(index, true, context).block;
    }

    public void setBlock(int index, byte[] block, ActionContext context) {
	Block b = getBlock(index, true, context);
	b.marked = true;
	// log.info("Block size of block " + index + " was " + b.block.length +
	// ", now becomes " + block.length, new Throwable());
	b.block = block;
    }

    private Block getBlock(int i, boolean removeOldest, ActionContext context) {
	synchronized (blocksCache) {
	    Block block = blocksCache.get(myId + i);
	    if (block == null) {
		FirstLayer.getIOLock();
		File f = null;
		try {
		    long time = System.currentTimeMillis();
		    f = getCacheFile("" + (i / 1000) + "/" + i);
		    if (log.isDebugEnabled()) {
			log.debug("Reading block " + f.getPath());
		    }
		    InputStream fin = fi.createInputStream(f);
		    InputStream stream = codec.createInputStream(fin);
		    DataInputStream din = new DataInputStream(stream);
		    // DataInputStream din = new DataInputStream(fin);
		    block = new Block();
		    int sz = din.readInt();
		    block.block = new byte[sz];
		    block.index = myId + i;
		    blocksCache.addBlock(block);
		    din.readFully(block.block);
		    din.close();
		    fin.close();
		    if (context != null) {
			/*
			 * if (me == -1) { me =
			 * context.getNetworkLayer().getMyPartition(); }
			 * context.incrCounter( "Node " + me +
			 * ", time spent reading block from disk",
			 * System.currentTimeMillis() - time);
			 * context.incrCounter("Node " + me +
			 * ", bytes read from disk", sz+4);
			 */
			context.incrCounter(
				"Time spent reading block from disk",
				System.currentTimeMillis() - time);
			context.incrCounter("Bytes read from disk", sz + 4);
		    }
		} catch (Exception e) {
		    log.error("Failed reading cache file " + f.getPath(), e);
		} finally {
		    FirstLayer.releaseIOLock();
		}

		if (removeOldest
			&& blocksCache.size() >= MAX_BUFFERS_TO_KEEP_CACHE) {
		    Block b = blocksCache.removeOldestBlockInCache();
		    if (b.marked) {
			writeBlockToDisk(b);
			b.marked = false;
		    }
		}
	    } else {
		blocksCache.moveToEnd(block);
	    }

	    return block;
	}
    }

    private final LongMap<int[]> cache = new LongMap<int[]>();

    // private LinkedList<Long> timeCache = new LinkedList<Long>();
    //
    private int[] getFromCache(long key) {
	return cache.get(key);
    }

    private void addToCache(long value, int[] v) {
	// TODO: when cache gets too large (say, more than 16M entries),
	// clean up half of it.
	if (!disableSubjectCache) {
	    cache.put(value, v);
	    if ((cache.size() & (0x100000 - 1)) == 0) {
		log.warn("Cache size = " + cache.size());
	    }
	}
    }

    private final int[] notPresent = new int[0];

    public int lastBlockNo;

    private static String[] cacheDirs = new String[6];

    public FirstLayer.ListBlocks getListBlocks(long resource,
	    ActionContext context) throws Exception {
	return s1.getBlock(resource, context);
    }

    public synchronized boolean getInitialAddress(long resource,
	    int[] coordinates, ActionContext context) throws Exception {
	int[] c = getFromCache(resource);
	if (c != null) {
	    if (c == notPresent) {
		return false;
	    }
	    System.arraycopy(c, 0, coordinates, 0, coordinates.length);
	    return true;
	}

	boolean retval = s1.getInf(resource, coordinates, context);
	if (retval) {
	    c = new int[coordinates.length];
	    System.arraycopy(coordinates, 0, c, 0, c.length);
	    addToCache(resource, c);
	    return true;
	}
	addToCache(resource, notPresent);
	return false;
    }

    public void loadIndexFromCache(String cachePath, String globalCachePath)
	    throws Exception {
	if (cachePath == null) {
	    cachePath = globalCachePath;
	}
	cacheDir = cachePath;
	globalCacheDir = globalCachePath;
	cacheDirs[(int) (myId >> 32)] = cacheDir;
	File dir = fi.createFile(globalCachePath);
	String[] lb = dir.list(new FilenameFilter() {
	    @Override
	    public boolean accept(File arg0, String arg1) {
		return arg1.startsWith("lb");
	    }
	});
	s1.setListBlockNames(lb);
	String[] blockDirs = dir.list(new FilenameFilter() {
	    @Override
	    public boolean accept(File arg0, String arg1) {
		Matcher m = p.matcher(arg1);
		return m.matches();
	    }
	});
	Arrays.sort(blockDirs, FirstLayer.lbComparator);
	dir = fi.createFile(dir + "/" + blockDirs[blockDirs.length - 1]);
	String[] blocks = dir.list();
	Arrays.sort(blocks, FirstLayer.lbComparator);
	lastBlockNo = Integer.valueOf(blockDirs[blockDirs.length - 1]) * 1000
		+ Integer.valueOf(blocks[blocks.length - 1]);
    }

    public byte[] getLastBlock() {
	return getBlock(lastBlockNo, null);
    }

    @Override
    public String toString() {
	return cacheDir;
    }

    public void startWritingCache(String cachePath) throws Exception {
	// Create the directory
	File dir = fi.createFile(cachePath);
	dir.mkdirs();
	cacheDir = cachePath;
	cacheDirs[(int) (myId >> 32)] = cacheDir;
    }

    public void closeWritingCache() throws Exception {
	writeSeparator(FLAG_END_SEQUENCE);

	// Write the remaining in the cache
	if (offset > 0) {
	    // Make sure that someone reading this block will see the end in
	    // time ...
	    byte[] newBlock = new byte[offset + 6];
	    System.arraycopy(currentBlock, 0, newBlock, 0, offset);
	    currentBlock = newBlock;
	    cacheBlockForWriting(true, usedBlocks - 1, currentBlock);
	    usedBlocks++;
	}

	// Write the metainformation in the cache dir
	if (cacheDir != null) {
	    s1.writeTo();
	} else {
	    throw new IOException("Cache dir was not set");
	}
    }

    protected File getCacheFile(String name) throws Exception {
	File f = fi.createFile(cacheDir + File.separator + name);
	if (!f.exists()) {
	    if (globalCacheDir != null && !globalCacheDir.equals(cacheDir)) {
		if (log.isDebugEnabled()) {
		    log.debug("Copying " + globalCacheDir + "/" + name + " to "
			    + cacheDir);
		}

		if (!f.getParentFile().exists())
		    f.getParentFile().mkdirs();

		try {
		    Thread.sleep(200); // Slow down ...
		} catch (InterruptedException e1) {
		    // ignore
		}
		try {
		    File gatFile = fi.createFile(globalCacheDir + "/" + name);
		    File f1 = fi.createFile(cacheDir + File.separator + name
			    + "_XXX");
		    // First copy to a file with a different name, and then
		    // rename,
		    // because otherwise, when the program crashes for some
		    // reason,
		    // during copying, the local cache may be inconsistent.
		    fi.copyTo(gatFile, f1);
		    f1.renameTo(f);
		} catch (Throwable e) {
		    if (log.isDebugEnabled()) {
			log.debug("Got ignored exception ", e);
		    }
		}
	    }
	}
	return f;
    }

    public void checkPosition(int currentBlockIndex, int currentOffset) {
	checkPosition(currentBlockIndex, currentOffset, false);
    }

    private void checkPosition(int cb, int co, boolean verbose) {
	int currentBlockIndex = cb;
	int currentOffset = co;
	try {
	    int[] temp = new int[2];
	    while (currentBlockIndex != Integer.MIN_VALUE) {
		if (verbose) {
		    log.info("Getting block " + currentBlockIndex);
		}
		byte[] block = getBlock(currentBlockIndex, null);
		if (block == null) {
		    log.error("Error in position", new Throwable());
		    System.exit(1);
		}
		/*
		 * log.info("checkPosition: currentBlockIndex = " +
		 * currentBlockIndex + ", currentOffset = " + currentOffset +
		 * ", block.length = " + block.length);
		 */
		if (currentOffset > (block.length - 8)) {
		    if (verbose) {
			log.info("currentOffset was " + currentOffset
				+ ", getting block " + currentBlockIndex + 1);
		    }
		    currentOffset = 0;
		    block = getBlock(++currentBlockIndex, null);
		}

		temp[0] = currentOffset;
		long value = Utils.decodeLong2(block, temp);
		currentOffset = temp[1];
		if (verbose) {
		    log.info("read predicate " + value);
		}
		if (currentOffset > (block.length - 8)) {
		    if (verbose) {
			log.info("currentOffset was " + currentOffset
				+ ", getting block " + currentBlockIndex + 1);
		    }
		    currentOffset = 0;
		    block = getBlock(++currentBlockIndex, null);
		}
		int newIndex = Utils.decodeInt(block, currentOffset);
		int newOff = Utils.decodeInt(block, currentOffset + 4);
		currentOffset += 8;
		if (newIndex == currentBlockIndex) {
		    newOff += currentOffset;
		}
		currentBlockIndex = newIndex;
		currentOffset = newOff;
		if (verbose) {
		    log.info("Read position: block = " + currentBlockIndex
			    + ", offset = " + currentOffset);
		}
	    }
	} catch (Throwable e) {
	    if (!verbose) {
		checkPosition(cb, co, true);
		log.error("Got Exception", e);
		System.exit(1);
	    }
	}
    }

    public void flushMarkedBuffers() throws Exception {
	synchronized (blocksCache) {
	    Block b = blocksCache.removeFromHead();
	    while (b != null) {
		if (b.marked && (b.index >> 32) == (myId >> 32)) {
		    writeBlockToDisk(b);
		}
		b = blocksCache.removeFromHead();
	    }
	    s1.flushListBlocks();
	}
    }

    public boolean getSubjectPosition(long subject, int[] position)
	    throws Exception {
	return s1.getSubjectPosition(subject, position);
    }

    public void shiftListBlocks(long subject, int currentBlockIndex, int len)
	    throws Exception {
	s1.shiftListBlocks(subject, currentBlockIndex, len);
    }

    public void updateLB(long subject, int[] pos) throws Exception {
	// checkPosition(pos[0], pos[1]);
	s1.updateLB(subject, pos);
    }

    public static void reset() {
	idCounter = 0;
    }
}
