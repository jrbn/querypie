package nl.vu.cs.querypie.utils;

import java.io.File;

import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.querypie.storage.disk.FilesInterface;
import nl.vu.cs.querypie.storage.disk.PlainTripleFile;
import nl.vu.cs.querypie.storage.disk.TripleFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirstElementsListCreator {

    static final Logger log = LoggerFactory
	    .getLogger(FirstElementsListCreator.class);

    static public void main(String[] args) {

	// Creates a file containing the first element of each of the
	// triple files.
	// Having such a file may help speed up the start up of the
	// cluster, especially
	// when it is running on a different site than the triple files.

	// Read index in args[0].

	if (args.length != 1) {
	    log.error("FirstElementsListCreator needs an argument: the database, which should have an index/spo and index/sop directory");
	    System.exit(1);
	}
	Configuration conf = new Configuration();
	// conf.set(Consts.STORAGE_IMPL, RDFStorage.class.getName());
	conf.set("indexFileImpl", PlainTripleFile.class.getName());
	runOnIndex(conf, args[0] + File.separator + "index" + File.separator
		+ "spo");
	runOnIndex(conf, args[0] + File.separator + "index" + File.separator
		+ "sop");
	runOnIndex(conf, args[0] + File.separator + "index" + File.separator
		+ "pos");
	runOnIndex(conf, args[0] + File.separator + "index" + File.separator
		+ "pso");
	runOnIndex(conf, args[0] + File.separator + "index" + File.separator
		+ "osp");
	runOnIndex(conf, args[0] + File.separator + "index" + File.separator
		+ "ops");
    }

    public static void runOnIndex(Configuration conf, String directory) {
	TripleFile[] files = Utils.getListFiles(conf, directory, true);

	File dir = new File(directory);
	if (!dir.exists()) {
	    log.error("Directory " + directory + " does not exist");
	    System.exit(1);
	}
	if (files.length == 0) {
	    log.error("Directory " + directory
		    + " does not contain triple files");
	    System.exit(1);
	}

	TripleFile newFile = Utils.getFirstElementsFile(conf, directory,
		new FilesInterface());
	if (newFile == null) {
	    log.error("Could not create TripleFile object for FirstElementsFile");
	    System.exit(1);
	}
	newFile.openToWrite();
	for (TripleFile file : files) {
	    log.info("Process file " + file.getName());
	    file.open();
	    if (file.next()) {
		byte[] triple = file.getTriple();
		newFile.writeTriple(triple, triple.length);
	    } else {
		log.warn(file.getName() + " does not contain any elements!");
		newFile.write(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
	    }
	    file.close();
	}
	newFile.close();
    }
}
