package nl.vu.cs.querypie.experiments;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.AjiraClient;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.querypie.Query;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.disk.TripleFile;
import nl.vu.cs.querypie.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAndInsertTriplesInIndex {

    public class Triple {

        long subject;
        long predicate;
        long object;

        public long getSubject() {
            return subject;
        }

        public void setSubject(long subject) {
            this.subject = subject;
        }

        public long getPredicate() {
            return predicate;
        }

        public void setPredicate(long predicate) {
            this.predicate = predicate;
        }

        public long getObject() {
            return object;
        }

        public void setObject(long object) {
            this.object = object;
        }

        @Override
        public int hashCode() {
            long l = subject + predicate + object;
            return ((int) (l >>> 32) ^ (int) l);
            // return toString().hashCode();
        }

        @Override
        public boolean equals(Object triple) {
            return ((Triple) triple).subject == subject
                    && ((Triple) triple).predicate == predicate
                    && ((Triple) triple).object == object;
        }

        @Override
        public String toString() {
            return subject + " " + predicate + " " + object;
        }
    }

    static final Logger log = LoggerFactory
            .getLogger(QueryAndInsertTriplesInIndex.class);

    TLong subject = new TLong();
    TLong predicate = new TLong();
    TLong object = new TLong();

    Query query = new Query();

    private String configurationFile;

    private long updateClosure(Configuration conf, Set<Triple> output_set,
            String closure, String query, boolean equivalent, boolean rules)
            throws Exception {
        log.debug("Process " + closure);

        long timeQuery = 0;

        AjiraClient is = this.query.getQueryResults(configurationFile, query,
                rules, false, false, rules);
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        boolean isFinished = is.getResult(tuples);
        double time = is.getTime();
        timeQuery += (long) time;
        log.debug("Received data (new " + tuples.size() + " tuples)");
        Set<Triple> newTriples = new HashSet<Triple>();
        long nTuples = tuples.size();

        while (tuples.size() > 0) {
            Tuple row = tuples.remove(tuples.size() - 1);
            Triple triple = new Triple();
            triple.setSubject(((RDFTerm) row.get(0)).getValue());
            triple.setPredicate(((RDFTerm) row.get(1)).getValue());
            triple.setObject(((RDFTerm) row.get(2)).getValue());
            newTriples.add(triple);
        }

        while (!isFinished) {
            // Fetch more triples from key
            tuples.clear();
            isFinished = is.getMoreResults(tuples);
            log.debug("Received more data (new " + tuples.size()
                    + " tuples) finished: " + isFinished);
            nTuples += tuples.size();
            while (tuples.size() > 0) {
                Tuple tuple = tuples.remove(tuples.size() - 1);
                Triple triple = new Triple();
                triple.setSubject(((TLong) tuple.get(0)).getValue());
                triple.setPredicate(((TLong) tuple.get(1)).getValue());
                triple.setObject(((TLong) tuple.get(2)).getValue());
                // if (!newTriples.contains(triple)) {
                newTriples.add(triple);
                // }
            }
        }

        output_set.addAll(newTriples);

        log.debug("Query executed in (ms.) " + time + " # elements " + nTuples
                + " new triples " + newTriples.size());

        return timeQuery;

    }

    public void run(Configuration conf, String[] args) throws Exception {
        long time = System.currentTimeMillis();
        Set<Triple> set = new HashSet<Triple>();
        Set<Triple> existingTriples = new HashSet<Triple>();

        query.intermediateStats = true;

        long reasoningTime = updateClosure(conf, existingTriples, null,
                "-1 -1 26388289431553", false, false);
        reasoningTime += updateClosure(conf, existingTriples, null,
                "-1 229420 16402", false, false);
        reasoningTime += updateClosure(conf, existingTriples, null,
                "62672173361275 1099553623948 245764", false, false);
        reasoningTime += updateClosure(conf, existingTriples, null,
                "-1 -1 3298551498228", false, false);
        reasoningTime += updateClosure(conf, existingTriples, null,
                "-1 303137 56075104315877", false, false);
        reasoningTime += updateClosure(conf, existingTriples, null,
                "-1 0 294944", false, false);

        // Repeat the same queries with rules activated
        reasoningTime += updateClosure(conf, set, null, "-1 -1 26388289431553",
                false, true);
        reasoningTime += updateClosure(conf, set, null, "-1 229420 16402",
                false, true);
        reasoningTime += updateClosure(conf, set, null,
                "62672173361275 1099553623948 245764", false, true);
        reasoningTime += updateClosure(conf, set, null, "-1 -1 3298551498228",
                false, true);
        reasoningTime += updateClosure(conf, set, null,
                "-1 303137 56075104315877", false, true);
        reasoningTime += updateClosure(conf, set, null, "-1 0 294944", false,
                true);

        log.info("Existing triples" + existingTriples.size());
        log.info("New triples" + set.size());

        set.removeAll(existingTriples);

        log.info("Total new derived triples: " + set.size());
        log.info("Total reasoning time: " + reasoningTime);

        if (set.size() > 0) {
            updateIndexes(conf, set);
        }

        log.info("Time to calculate schema and update indexes = "
                + (System.currentTimeMillis() - time));
    }

    private byte[][] sortTriplesAccordingToIndex(String index,
            Set<Triple> triples) {
        // Sort the new triples according the index
        ArrayList<byte[]> list = new ArrayList<byte[]>();
        for (Triple triple : triples) {
            byte[] el = new byte[24];
            switch (index.charAt(0)) {
            case 's':
                Utils.encodeLong(el, 0, triple.getSubject());
                break;
            case 'p':
                Utils.encodeLong(el, 0, triple.getPredicate());
                break;
            case 'o':
                Utils.encodeLong(el, 0, triple.getObject());
                break;
            }

            switch (index.charAt(1)) {
            case 's':
                Utils.encodeLong(el, 8, triple.getSubject());
                break;
            case 'p':
                Utils.encodeLong(el, 8, triple.getPredicate());
                break;
            case 'o':
                Utils.encodeLong(el, 8, triple.getObject());
                break;
            }

            switch (index.charAt(2)) {
            case 's':
                Utils.encodeLong(el, 16, triple.getSubject());
                break;
            case 'p':
                Utils.encodeLong(el, 16, triple.getPredicate());
                break;
            case 'o':
                Utils.encodeLong(el, 16, triple.getObject());
                break;
            }
            list.add(el);
        }
        byte[][] sortedList = list.toArray(new byte[list.size()][]);
        Arrays.sort(sortedList, new Utils.BytesComparator());

        return sortedList;

    }

    private void updateIndexes(Configuration conf, Set<Triple> newTriples)
            throws IOException, IllegalArgumentException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        File indexesDir = new File(conf.get("input.indexesDir", ""));
        File[] indexes = indexesDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });
        Constructor<? extends TripleFile> constr = Utils
                .getTripleFileImplementation(conf);

        for (File index : indexes) {
            log.info("Process index " + index.getName());

            File cache = new File(index.getAbsoluteFile() + "/_cache");
            if (cache.exists()) {
                log.debug("Delete cache file ...");
                Utils.deleteDir(cache);
            }

            log.debug("Sorting triples according to the index ...");
            byte[][] triples = sortTriplesAccordingToIndex(index.getName(),
                    newTriples);

            TripleFile[] chunks = Utils.getListFiles(conf,
                    index.getAbsolutePath(), true);

            // The index is sorted only between partitions. Will be much
            // faster because it will insert it only at partition 0
            updateIndex(triples, chunks, constr);

            // if (index.equals("pos") || index.equals("ops")) {
            //
            // } else {
            // updateIndex(triples, chunks, constr);
            // }

            // String list = "";
            // for(TripleFile file : chunks) {
            // list+= file.getName() + ",";
            // }
            // log.debug("Files to process: " + list);

        }
    }

    private void updateIndex(byte[][] triples, TripleFile[] chunks,
            Constructor<? extends TripleFile> constr)
            throws IllegalArgumentException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        int currentChunk = 0;
        byte[] beginningNextChunk = null;
        boolean isEOF = false;

        Comparator<byte[]> comparator = new Utils.BytesComparator();
        TripleFile oldFile = null;
        TripleFile newFile = null;

        int i = 0;
        while (i < triples.length) {
            byte[] tripleToInsert = triples[i++];

            /*** Go to the correct chunk ***/
            while (currentChunk < chunks.length
                    && (beginningNextChunk == null || comparator.compare(
                            tripleToInsert, beginningNextChunk) > 0)) {

                // Flush the previous file
                if (oldFile != null && !isEOF) {
                    log.debug("Flush the file " + oldFile.getName() + " ...");
                    oldFile.copyTo(newFile);
                    oldFile.close();
                    // oldFile.delete();
                    newFile.close();
                    oldFile = null;
                    newFile = null;
                }

                // Go to the next chunk
                currentChunk++;
                if (currentChunk < chunks.length) {
                    TripleFile file = chunks[currentChunk];
                    file.open();
                    if (file.next()) {
                        beginningNextChunk = file.getTriple();
                    }
                    file.close();
                }
            }

            // Rename the chunk we will replace
            if (newFile == null) {
                oldFile = chunks[Math.max(0, currentChunk - 1)];
                String originalName = oldFile.getPath();

                File file = new File(originalName);
                String toRename = file.getParent() + "/_" + file.getName()
                        + ".old";
                oldFile.renameTo(toRename);

                log.debug("Rename file " + originalName);

                newFile = constr.newInstance(originalName);
                newFile.openToWrite();
                oldFile.open();
                isEOF = !oldFile.next();
            }

            /*** Go to the correct position in the file ***/
            while (!isEOF
                    && comparator.compare(tripleToInsert, oldFile.getTriple()) > 0) {

                newFile.writeTriple(oldFile.getTriple(), 24);
                isEOF = !oldFile.next();
                if (isEOF) {
                    oldFile.close();
                    // oldFile.delete();
                    oldFile = null;
                }
            }

            newFile.writeTriple(tripleToInsert, 24);
        }

        /*** Flush the last file ***/
        if (oldFile != null) {
            if (!isEOF) {
                do {
                    newFile.writeTriple(oldFile.getTriple(), 24);
                } while (oldFile.next());
            }
            oldFile.close();
            // oldFile.delete();
        }

        if (newFile != null) {
            newFile.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out
                    .println("Usage: CalculateClosure <input data> --rules <file rules>");
            return;
        }

        Configuration conf = new Configuration();
        /*
         * Why bother setting it? Completely ignored anyway.
         * conf.set(InputLayer.INPUT_LAYER_CLASS, RDFStorage.class.getName());
         * conf.set("indexFileImpl", PlainTripleFile.class.getName());
         * conf.set("input.closureDir", args[0] + "/closure");
         * conf.set("input.schemaDir", args[0] + "/closure");
         * conf.set("input.indexesDir", args[0] + "/index");
         */

        new QueryAndInsertTriplesInIndex().run(conf, args);
    }
}
