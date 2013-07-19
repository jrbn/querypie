package arch.datalayer.files;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;

public class MultiFilesReader extends TupleIterator {

    static final Logger log = LoggerFactory.getLogger(MultiFilesReader.class);

    Constructor<? extends FileIterator> cfileReader;
    List<File> listFiles = new ArrayList<File>();

    int currentIndex = 0;
    FileIterator currentItr = null;

    private static void recursiveListint(List<File> list, File file,
	    FilenameFilter filter) {
	if (file.isFile()) {
	    list.add(file);
	} else {
	    File[] children = null;
	    if (filter != null) {
		children = file.listFiles(filter);
	    } else {
		children = file.listFiles();
	    }

	    for (File child : children) {
		if (child.isFile()) {
		    list.add(child);
		} else {
		    recursiveListint(list, child, filter);
		}
	    }
	}
    }

    public static List<File> listAllFiles(String path, final String filterClass) {
	List<File> list = new ArrayList<File>();
	File file = new File(path);
	if (file.isDirectory()) {
	    // Get all files
	    if (filterClass != null) {
		try {
		    Class<? extends FilenameFilter> clazz = Class.forName(
			    filterClass).asSubclass(FilenameFilter.class);
		    recursiveListint(list, file, clazz.newInstance());
		} catch (Exception e) {
		    log.error("Couldn't instantiate filter " + filterClass
			    + ". Ignore it.");
		    recursiveListint(list, file, null);
		}
	    } else {
		recursiveListint(list, file, null);
	    }

	} else if (file.exists()) {
	    list.add(file);
	}
	return list;
    }

    public MultiFilesReader(String path, String filter,
	    Class<? extends FileIterator> cfileReader)
	    throws SecurityException, NoSuchMethodException {
	this.cfileReader = cfileReader.getConstructor(File.class);
	log.debug("Input: " + path);
	if (path != null) {
	    String[] splits = path.split(":");
	    List<File> list = listAllFiles(splits[0], filter);
	    if (splits.length > 1) {
		// There is an interval to return
		int begin = Integer.valueOf(splits[1]);
		int end = Integer.valueOf(splits[2]);
		listFiles = new ArrayList<File>();
		for (int i = begin; i < end; ++i) {
		    listFiles.add(list.get(i));
		}
	    } else {
		listFiles = list;
	    }
	}
    }

    @Override
    public boolean next() throws Exception {
	if (currentItr == null || !currentItr.next()) {
	    if (currentIndex < listFiles.size()) {
		File file = listFiles.get(currentIndex++);
		currentItr = cfileReader.newInstance(file);
		if (currentItr != null) {
		    return next();
		} else {
		    return false;
		}
	    }
	    return false;
	}

	return true;
    }

    @Override
    public void getTuple(Tuple tuple) throws Exception {
	currentItr.getTuple(tuple);
    }

    @Override
    public boolean isReady() {
	return true;
    }
}
