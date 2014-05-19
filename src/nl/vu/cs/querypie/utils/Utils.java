package nl.vu.cs.querypie.utils;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.disk.FilesInterface;
import nl.vu.cs.querypie.storage.disk.TripleFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils extends nl.vu.cs.ajira.utils.Utils {

    static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static TripleFile[] sortFiles(TripleFile[] files) {
	for (int i = 0; i < files.length - 1; ++i) {
	    String fileName = files[i].getName();

	    int partid = Integer.valueOf(fileName.substring(
		    fileName.lastIndexOf('-') + 1, fileName.lastIndexOf('_')));
	    int fileId = Integer.valueOf(fileName.substring(fileName
		    .lastIndexOf('_') + 1));
	    int minIndex = i;

	    for (int j = i + 1; j < files.length; ++j) {
		String currentFileName = files[j].getName();

		try {
		    int currentPartId = Integer.valueOf(currentFileName
			    .substring(currentFileName.lastIndexOf('-') + 1,
				    currentFileName.lastIndexOf('_')));
		    int currentFileId = Integer.valueOf(currentFileName
			    .substring(currentFileName.lastIndexOf('_') + 1));

		    if (currentPartId < partid
			    || (currentPartId == partid && currentFileId < fileId)) {
			fileId = currentFileId;
			partid = currentPartId;
			minIndex = j;
		    }
		} catch (Exception e) {
		    log.error("Failed parsing file " + currentFileName, e);
		}
	    }

	    if (minIndex != i) {
		// Swap the two elements
		TripleFile box = files[minIndex];
		files[minIndex] = files[i];
		files[i] = box;
	    }
	}

	return files;
    }

    public static Constructor<? extends TripleFile> getTripleFileImplementation(
	    Configuration conf) {
	try {
	    String className = conf.get("indexFileImpl", "");
	    Class<? extends TripleFile> indexFileImpl = ClassLoader
		    .getSystemClassLoader().loadClass(className)
		    .asSubclass(TripleFile.class);
	    return indexFileImpl.getConstructor(String.class);
	} catch (Exception e) {
	    log.error("Failed loading implemenation of fileIndex", e);
	}

	return null;
    }

    public static Constructor<? extends TripleFile> getTripleFileImplementationWithFi(
	    Configuration conf) {
	try {
	    String className = conf.get("indexFileImpl", "");
	    Class<? extends TripleFile> indexFileImpl = ClassLoader
		    .getSystemClassLoader().loadClass(className)
		    .asSubclass(TripleFile.class);
	    return indexFileImpl.getConstructor(String.class,
		    FilesInterface.class);
	} catch (Exception e) {
	    log.error("Failed loading implemenation of fileIndex", e);
	}

	return null;
    }

    public static TripleFile getFirstElementsFile(Configuration conf,
	    String path, FilesInterface fi) {

	Constructor<? extends TripleFile> constructor = Utils
		.getTripleFileImplementationWithFi(conf);
	try {
	    return constructor.newInstance(path + "/_FirstElementsList", fi);
	} catch (Throwable e) {
	    return null;
	}
    }

    public static TripleFile[] getListFiles(Configuration conf, String path,
	    boolean sort) {

	File dir = new File(path);
	if (!dir.exists()) {
	    return new TripleFile[0];
	}

	Constructor<? extends TripleFile> constructor = getTripleFileImplementation(conf);
	ArrayList<TripleFile> list = new ArrayList<TripleFile>();

	for (File file : dir.listFiles()) {
	    String fileName = file.getName();
	    if (!fileName.startsWith("_") && !fileName.startsWith(".")) {
		try {
		    TripleFile newFile = constructor.newInstance(file
			    .getAbsolutePath());
		    list.add(newFile);
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	}

	if (sort) {
	    TripleFile[] alist = list.toArray(new TripleFile[list.size()]);

	    Arrays.sort(alist, new Comparator<TripleFile>() {
		@Override
		public int compare(TripleFile o1, TripleFile o2) {
		    return o1.getName().compareTo(o2.getName());
		}
	    });

	    return alist;
	} else {
	    return list.toArray(new TripleFile[list.size()]);
	}
    }

    public static void deleteDir(File file) {
	if (file.isDirectory()) {
	    for (File child : file.listFiles()) {
		deleteDir(child);
	    }
	}
	file.delete();
    }

    public static Pattern parsePattern(String sp) {
	Pattern p = new Pattern();
	String[] sterms = sp.split(" ");
	for (int i = 0; i < 3; ++i) {
	    RDFTerm t = new RDFTerm();
	    String st = sterms[i];
	    if (st.charAt(0) == '?') {
		t.setName(st.substring(1));
		t.setValue(Schema.ALL_RESOURCES);
	    } else {
		t.setName(null);
		t.setValue(Long.valueOf(st));
	    }
	    p.p[i] = t;
	}
	return p;

    }
}