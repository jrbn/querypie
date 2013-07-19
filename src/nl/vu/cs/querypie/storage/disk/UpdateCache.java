package nl.vu.cs.querypie.storage.disk;

import ibis.util.ThreadPool;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;

import nl.vu.cs.querypie.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.utils.Configuration;

public class UpdateCache {
    
    static final Logger log = LoggerFactory.getLogger(UpdateCache.class);
    
    public static final String[] indices = { "ops", "osp", "pos", "pso", "sop", "spo" };

    private static int counter;

    File cacheDir;
    int[] partitions;
    ArrayList<byte[]> triples;
    int nAvailablePartitions;
    int readBlockIndex = -1;
    int readOffset = -1;
    byte[] readBlock;
    Index index;
    FilesInterface fi;
    Constructor<? extends TripleFile> constr;
    String partitionName;
    int nodeNo;
    byte[] writeBlock = new byte[2*Index.BLOCK_SIZE];
    int writeOffset;
    int added;
    
    private int nNodes;
    
    public UpdateCache(Configuration conf, String partition, String triplesFile, String fileFile, int myNode, int nNodes) throws Exception {
 
        fi = (FilesInterface) Class.forName(
                conf.get(RDFStorage.FILES_INTERFACE,
                        FilesInterface.class.getName())).newInstance();
        String sep = fi.getFilesSeparator();
        String indexDir = conf.get("input.indexesDir", "");
        String cache = conf.get(RDFStorage.CACHE_LOCATION, indexDir);
        indexDir = indexDir + sep + partition;
        cache = cache + sep + partition + sep + "_cache";
        
        if (log.isDebugEnabled()) {
            log.debug("indexDir = " + indexDir + ", cacheDir = " + cache);
        }
        
        this.partitionName = partition;
        this.nodeNo = myNode;
        this.nNodes = nNodes;
        
        triplesFile = triplesFile + "." + partition;
        fileFile = fileFile + "." + partition;
        
        constr = Utils.getTripleFileImplementation(conf);

        triples = new ArrayList<byte[]>();
        TripleFile f = constr.newInstance(triplesFile);
        f.open();
        while (f.next()) {
            byte[] t = f.getTriple();
            byte[] triple = new byte[t.length];
            System.arraycopy(t, 0, triple, 0, t.length);
            triples.add(triple);
        }
        f.close();
        partitions = new int[triples.size()];
        DataInputStream d = new DataInputStream(new FileInputStream(fileFile));
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = d.readInt();
        }
        d.close();

        TripleFile[] tripleFiles = fi.getListFiles(conf, indexDir, true);
        /* Calculate the number of partitions per node */
        TripleFile lastFile = tripleFiles[tripleFiles.length - 1];
        nAvailablePartitions = Integer.valueOf(lastFile.getName()
                .substring(lastFile.getName().lastIndexOf('-') + 1,
                        lastFile.getName().lastIndexOf('_'))) + 1;
        cacheDir = fi.createFile(cache);
    }
    
    int firstPartition;
    int lastPartition;
    int tripleIndex;
    
    public long[] triple = new long[3];
    
    public long[] getTriple() {
        long[] retval = null;
        for (; tripleIndex < partitions.length; tripleIndex++) {
            int part = partitions[tripleIndex];
            if (part < firstPartition) {
                continue;
            }
            if (part > lastPartition) {
                return null;
            }
            byte[] btriple = triples.get(tripleIndex++);
            triple[0] = Utils.decodeLong(btriple, 0);
            triple[1] = Utils.decodeLong(btriple, 8);
            triple[2] = Utils.decodeLong(btriple, 16);
            retval = triple;
            log.debug("Triple = [" + triple[0] + ", " + triple[1] + ", " + triple[2] + "]");
            break;
        }
        return retval;
    }
    
    
    public void updateCache() throws Exception {
        int nPartitionsPerNode = nAvailablePartitions / nNodes;
        firstPartition = nodeNo * nPartitionsPerNode;
        lastPartition = (nodeNo < nNodes - 1 ? (nodeNo + 1)
                * nPartitionsPerNode : nAvailablePartitions) - 1;
        
        if (log.isInfoEnabled()) {
            log.info("nPartitionsPerNode = " + nPartitionsPerNode + ", firstPartition = " + firstPartition + ", lastPartition = " + lastPartition);
        }
        if (partitions.length == 0) {
            return;
        }

        String subDir = "" + nodeNo + "_" + nPartitionsPerNode;
        
        index = new Index(partitionName, null, false);
        index.setFilesInterface(fi);
        index.disableSubjectCache();
        index.loadIndexFromCache(null, cacheDir.getAbsolutePath() + fi.getFilesSeparator() + subDir);
        long[] t = getTriple();
        while (t != null) {
            long savedSubject = t[0];
            if (log.isDebugEnabled()) {
                index.getSubjectPosition(savedSubject, entry1Pos);
                log.debug("Got position [" + entry1Pos[0] + ", " + entry1Pos[1] + " ] for subject " + savedSubject);
                index.checkPosition(entry1Pos[0], entry1Pos[1]);
            }
            t = handleNextSubject(t);
            if (writeOffset != 0) {
                index.shiftListBlocks(savedSubject, readBlockIndex, added);
                flushWriteBlock();
            }
            if (log.isDebugEnabled()) {
                index.getSubjectPosition(savedSubject, entry1Pos);
                index.checkPosition(entry1Pos[0], entry1Pos[1]);
            }
        }
        index.flushMarkedBuffers();
    }
    
    public void flushWriteBlock() {
        if (log.isDebugEnabled()) {
            log.debug("flushWriteBlock, writeOffset = " + writeOffset + ", added = " + added);
        }
        if (writeOffset > 0) {
            int targetLen = readBlock.length + added;
            if (log.isDebugEnabled()) {
                log.debug("target length = " + targetLen);
            }
            byte[] targetBlock = new byte[targetLen];
            if (writeOffset > targetBlock.length) {
                log.error("Oops", new Throwable());
            }
            System.arraycopy(writeBlock, 0, targetBlock, 0, writeOffset);
            System.arraycopy(readBlock, writeOffset-added, targetBlock, writeOffset, targetLen - writeOffset);
            index.setBlock(readBlockIndex, targetBlock, null);
            added = 0;
            writeOffset = 0;
            if (log.isDebugEnabled()) {
                log.debug("Flushed block " + readBlockIndex);
            }
            readBlock = targetBlock;
        }
    }
    
    public void grow(int len) {
        int cr = writeBlock.length;
        while (cr < len + writeOffset) {
            cr <<= 1;
        }
        if (cr != writeBlock.length) {
            byte[] b = new byte[cr];
            System.arraycopy(writeBlock, 0, b, 0, writeOffset);
            writeBlock = b;
        }
    }
    
    public void setWritePosition(int off) {
//        if (log.isDebugEnabled()) {
//            log.debug("setWritePosition, off = " + off + ", writeOffset = " + writeOffset + ", addded = " + added, new Throwable());
//        }
        int len = off - writeOffset;
        if (len < 0) {
            log.error("Oops!", new Throwable());
        }
        grow(len);
        if (len > 0) {
            System.arraycopy(readBlock, writeOffset-added, writeBlock, writeOffset, len);
        }
        writeOffset = off;
    }
    
    public void addSeparator(byte sep) {
        grow(1);
//        if (log.isDebugEnabled()) {
//            log.debug("Adding separator " + sep + " at position " + writeOffset);
//        }
        writeBlock[writeOffset++] = sep;
        added++;
    }
    
    public void addTerm(long term) {
        grow(8);
        int saved = writeOffset;
//        if (log.isDebugEnabled()) {
//            log.debug("Adding term " + term + " at position " + writeOffset);
//        }
        writeOffset = Utils.encodeLong2(writeBlock, writeOffset, term);
        added += writeOffset - saved;
    }
    
    public void fixPositionInWriteBlock(int pos) {
        // pos refers to a position in the writeblock where we can find a position.
        // this must now refer to the current position.
//        if (log.isDebugEnabled()) {
//            log.debug("Fixing position at " + pos + " to [" + readBlockIndex + ", " + writeOffset + "]");
//        }
        Utils.encodeInt(writeBlock, pos, readBlockIndex);
        Utils.encodeInt(writeBlock, pos+4, writeOffset - (pos+8));
    }
    
    public void addPosition(int blockno, int offset) {
        grow(8);
//        if (log.isDebugEnabled()) {
//            log.debug("Adding position [" + blockno + ", " + offset + "] at position " + writeOffset);
//        }
        Utils.encodeInt(writeBlock, writeOffset, blockno);
        if (blockno == readBlockIndex) {
            Utils.encodeInt(writeBlock, writeOffset+4, offset - (writeOffset+8));
        } else {
            Utils.encodeInt(writeBlock, writeOffset+4, offset);
        }
        writeOffset += 8;
        added += 8;
    }
    
    public void readSpace(int sz) {
        if (readOffset > readBlock.length - sz) {
            setReadPosition(readBlockIndex+1, 0);
        }
    }
    
    public void getReadPos(int[] pos) {
        pos[0] = readBlockIndex;
        pos[1] = readOffset;
    }
    
    
    public void getWritePos(int[] pos) {
        pos[0] = readBlockIndex;
        pos[1] = readOffset + added;
    }

    
    long entry1 = -1;
    long entry2 = -1;
    long entry3 = -1;
    int[] entry1Pos = new int[2];
    int[] entry2Pos = new int[2];
    int[] posOfPos = new int[2];
    int[] nextEntry2Pos = new int[2];
    int[] flagPos = new int[2];
    
    public long[] newSubject(long[] triple) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Adding a completely new subject");
        }
        long subject = triple[0];
        entry1Pos[1] += added;
        setWritePosition(entry1Pos[1]);
        int nextPos = -1;
        long savedSubject = triple[0];
        while (savedSubject == triple[0]) {
            if (nextPos != -1) {
                addSeparator(Index.FLAG_NEXT_PREDICATE);
                fixPositionInWriteBlock(nextPos);
            }
            addTerm(triple[1]);
            long np = triple[1];
            nextPos = writeOffset;
            addPosition(Integer.MIN_VALUE, Integer.MIN_VALUE);
            addTerm(triple[2]);
            triple = getTriple();
            if (triple == null) {
                addSeparator(Index.FLAG_END_SEQUENCE);
                return triple;
            }
            while (savedSubject == triple[0] && np == triple[1]) {
                addSeparator(Index.FLAG_NEXT_OBJECT);
                addTerm(triple[2]);
                triple = getTriple();
                if (triple == null) {
                    addSeparator(Index.FLAG_END_SEQUENCE);
                    return triple;
                }
            }
        }
        addSeparator(Index.FLAG_END_SEQUENCE);
        index.updateLB(subject, entry1Pos);
        return triple;
    }
    
    public long[] insertPredicateSequence(long[] triple) {
        long np = triple[1];
        setWritePosition(entry2Pos[1]+added);
        addTerm(np);
        int nextPos = writeOffset;
        addPosition(Integer.MIN_VALUE, Integer.MIN_VALUE);
        addTerm(triple[2]);
        triple = getTriple();
        while (triple != null && triple[0] == entry1 && triple[1] == np) {
            addSeparator(Index.FLAG_NEXT_OBJECT);
            addTerm(triple[2]);
            triple = getTriple();
        }
        addSeparator(Index.FLAG_NEXT_PREDICATE);
        fixPositionInWriteBlock(nextPos);
        return triple;
    }
    
    public void findEndSequenceFlag(int flag) {
        // Sets flagPos to the position of the END_SEQUENCE flag.
        // We start from a read position that indicates an object.
        while (flag != Index.FLAG_END_SEQUENCE) {
            readTerm();
            readSpace(1);
            getWritePos(flagPos);
            flag = readSeparator();
        }
    }
    
    public long[] copyToEndOfSubject(long[] triple) {
        // Our read pointer indicates the END_SEQUENCE flag.
        // We have a new predicate, so we have to backpatch
        // its position into PosOfPos.
        int nextPos = -1;
        while (triple != null && triple[0] == entry1) {
            addSeparator(Index.FLAG_NEXT_PREDICATE);
            if (nextPos != -1) {
                fixPositionInWriteBlock(nextPos);
            }
            fixPosition(posOfPos, writeOffset);
            long np = triple[1];
            addTerm(triple[1]);
            nextPos = writeOffset;
            addPosition(Integer.MIN_VALUE, Integer.MIN_VALUE);
            addTerm(triple[2]);
            triple = getTriple();
            while (triple != null && triple[0] == entry1 && triple[1] == np) {
                addSeparator(Index.FLAG_NEXT_OBJECT);
                addTerm(triple[2]);
                triple = getTriple();
            }
        }
        return triple;
    }
    
    public void fixPosition(int[] posOfPos, int offset) {
        int block = posOfPos[0];
        byte[] b;
        if (block == readBlockIndex) {
            b = writeBlock;
            Utils.encodeInt(b, posOfPos[1], readBlockIndex);
            Utils.encodeInt(b, posOfPos[1]+4, offset - (posOfPos[1] + 8));
        } else {
            b = index.getBlock(block, null);
            Utils.encodeInt(b, posOfPos[1], readBlockIndex);
            Utils.encodeInt(b, posOfPos[1]+4, offset);
            index.setBlock(block, b, null);
        }
    }
    
    public long[] handleNextSubject(long[] triple) throws Exception {
        boolean subjectPresent = index.getSubjectPosition(triple[0], entry1Pos);
        setReadPosition(entry1Pos[0], entry1Pos[1]);
        if (! subjectPresent) {
            return newSubject(triple);
        }
        entry1 = triple[0];
        entry2Pos[0] = entry1Pos[0];
        entry2Pos[1] = entry1Pos[1];
        entry2 = readTerm();
        while (triple != null && triple[0] == entry1) {
            // Assumption at this point is that we just read an entry2.
            if (triple[1] < entry2) {
//                if (log.isDebugEnabled()) {
//                    log.debug("Inserting a predicate + objects in front of existing predicate" + entry2);
//                }
                // read position is still at entry2 after this ...
                triple = insertPredicateSequence(triple);
            } else {
                readSpace(8);
                getWritePos(posOfPos);
                readPosition(nextEntry2Pos);
                readSpace(8);
                if (triple[1] == entry2) {   
                    if (log.isDebugEnabled()) {
                        log.debug("Merging objects, predicate is equal");
                    }
                    int savedPos = readOffset;
                    entry3 = readTerm();
                    // Next is a separator
                    while (triple != null && triple[0] == entry1 && triple[1] == entry2) {
                        if (triple[2] <= entry3) {
                            if (triple[2] != entry3) {
                                setWritePosition(savedPos+added);
                                addTerm(triple[2]);
                                addSeparator(Index.FLAG_NEXT_OBJECT);
                            } else {
                                log.warn("Already present!");
                            }
                            triple = getTriple();
                            continue;
                        }
                        //  Still next is a separator
                        readSpace(1);
                        getWritePos(flagPos);
                        byte flag = readSeparator();                   
                        if (flag == Index.FLAG_NEXT_OBJECT) {
                            readSpace(8);
                            savedPos = readOffset;
                            entry3 = readTerm();
                            continue;
                        } else readOffset--;
                        // Still next is a separator
                        setWritePosition(flagPos[1]);
                        do {
                            addSeparator(Index.FLAG_NEXT_OBJECT);
                            addTerm(triple[2]);
                            triple = getTriple();
                        } while (triple != null && triple[0] == entry1 && triple[1] == entry2);
                    }
                    if (nextEntry2Pos[0] == Integer.MIN_VALUE) {
                        if (triple != null && triple[0] == entry1) {
                            if (log.isDebugEnabled()) {
                                log.debug("No more entry2-s, so finding the end and adding the reset of this subject");
                            }
                            findEndSequenceFlag(readSeparator());
                            return copyToEndOfSubject(triple);
                        }
                        return triple;
                    } else {
                        if (nextEntry2Pos[0] == readBlockIndex) {
                            if (posOfPos[0] == readBlockIndex) {
                                Utils.encodeInt(writeBlock, posOfPos[1]+4, nextEntry2Pos[1] + added - (posOfPos[1] + 8));
                            } else {
                                byte[] b = index.getBlock(posOfPos[0], null);
                                Utils.encodeInt(b, posOfPos[1]+4, nextEntry2Pos[1] + added);
                                index.setBlock(posOfPos[0], b, null);
                            }
                        }
                        setReadPosition(nextEntry2Pos[0], nextEntry2Pos[1]);
                        entry2Pos[0] = nextEntry2Pos[0];
                        entry2Pos[1] = nextEntry2Pos[1];
                        entry2 = readTerm();
                    }
                } else {
                    // entry2 < triple[1].
                    if (nextEntry2Pos[0] != Integer.MIN_VALUE) {
                        if (log.isDebugEnabled()) {
                            log.debug("Skipping to next entry2");
                        }
                        setReadPosition(nextEntry2Pos[0], nextEntry2Pos[1]);
                        readSpace(8);
                        getReadPos(entry2Pos);
                        entry2 = readTerm();
                        continue;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Skipping to end of subject and then adding stuff");
                    }
                    // No more entry2's, find end of objects.
                    findEndSequenceFlag(-1);
                    // And now add everything on this subject
                    setWritePosition(flagPos[1]);
                    triple = copyToEndOfSubject(triple);
                }   
            }
        }
        return triple;
    }

    int[] temp = new int[2];
    
    private void setReadPosition(int blockno, int offset) {
        if (readBlockIndex != blockno) {
            flushWriteBlock();
            if (log.isDebugEnabled()) {
                log.debug("Getting block " + blockno);
            }
            readBlockIndex = blockno;
            readBlock = index.getBlock(blockno, null);
        }
        if (log.isDebugEnabled()) {
            log.debug("Setting read offset " + offset);
        }
        readOffset = offset;
    }
    
    private void readPosition(int[] readPosition) {
        if (readOffset > (readBlock.length - 8)) {
            setReadPosition(readBlockIndex+1, 0);
         }
        readPosition[0] = Utils.decodeInt(readBlock, readOffset);
        readPosition[1] = Utils.decodeInt(readBlock, readOffset+4);
        readOffset += 8;
        if (readPosition[0] == readBlockIndex) {
            readPosition[1] += readOffset;
        }
    }

    private long readTerm() {
	if (readOffset > (readBlock.length - 8)) {
	    setReadPosition(readBlockIndex+1, 0);
	}

	temp[0] = readOffset;
	long value = Utils.decodeLong2(readBlock, temp);

	readOffset = temp[1];
	return value;
    }
    
    private byte readSeparator() {
	try {
	    byte retval = readBlock[readOffset++];
	    if (log.isDebugEnabled()) {
	        if (retval != Index.FLAG_END_SEQUENCE && retval != Index.FLAG_NEXT_OBJECT && retval != Index.FLAG_NEXT_PREDICATE) {
	            log.debug("OOPS! Wrong separator", new Throwable());
	        }
	    }
	    return retval;
	} catch (ArrayIndexOutOfBoundsException e) {
	    setReadPosition(readBlockIndex+1, 0);
	}
	return readBlock[readOffset++];
    }
        
    public static void main(String[] args) throws Exception {
        
        if (args.length < 6) {
            log.error("Usage: UpdateCache <indexDir> <cacheDir> <triples-update> <file-update> <nodeNo> <nNodes> [ --threads  ...]");
            return;
        }  
        
        long time = System.currentTimeMillis();
        
        // Don't use multiThreading yet, it does not work.
        
        boolean multiThreading = false;
          
        Configuration conf = new Configuration();
        conf.set("indexFileImpl", "reasoner.storagelayer.PlainTripleFile");
        conf.set("input.indexesDir", args[0] + "/index");
        conf.set(RDFStorage.CACHE_LOCATION, args[1]);
        for (int i = 6; i < args.length; i++) {
            if (args[i].equals("--threads")) {
                multiThreading = true;
            }
        }
        
        counter = indices.length;
        
        for (String s : indices) {
            final UpdateCache updater = new UpdateCache(conf, s, args[2], args[3], Integer.valueOf(args[4]), Integer.valueOf(args[5]));
            if (multiThreading) {
                final String partition = s;
                ThreadPool.createNew(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            updater.updateCache();
                        } catch (Throwable e) {
                            log.error("error", e);
                        }
                        synchronized (UpdateCache.class) {
                            counter--;
                            UpdateCache.class.notify();
                        }
                    }
                }, "Process " + partition);
            } else {
                updater.updateCache();
            }
        }
        if (multiThreading) {
            synchronized(UpdateCache.class) {
                while (counter > 0) {
                    try {
                        UpdateCache.class.wait();
                    } catch(InterruptedException e) {
                        // ignore
                    }
                }
            }
        }
        System.out.println("UpdateCache took " + (System.currentTimeMillis() - time) + " milliseconds.");
    }
}
