package nl.vu.cs.querypie.indices;

import java.io.File;

import nl.vu.cs.querypie.storage.RDFTerm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.Tuple;
import arch.datalayer.files.FileIterator;
import data.Triple;
import data.TripleSource;

public class CompressedReader extends FileIterator {

    protected static final Logger log = LoggerFactory
	    .getLogger(CompressedReader.class);

    private SequenceFile.Reader input;

    TripleSource source = new TripleSource();
    Triple triple = new Triple();
    RDFTerm t1 = new RDFTerm();
    RDFTerm t2 = new RDFTerm();
    RDFTerm t3 = new RDFTerm();

    public CompressedReader(File file) {
	super(file);
	input = null;
	try {
	    input = new SequenceFile.Reader(
		    FileSystem.get(new Configuration()), new Path(
			    file.getAbsolutePath()), new Configuration());
	} catch (Exception e) {
	    log.error("Failed reading file " + file);
	}
    }

    @Override
    public boolean next() throws Exception {
	if (input != null) {
	    if (input.next(source, triple)) {
		return true;
	    } else {
		input.close();
		return false;
	    }
	}
	return false;
    }

    @Override
    public void getTuple(Tuple tuple) throws Exception {
	t1.setValue(triple.getSubject());
	t2.setValue(triple.getPredicate());
	t3.setValue(triple.getObject());
	tuple.set(t1, t2, t3);
    }

}
