package nl.vu.cs.querypie.storage.disk;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.querypie.utils.Utils;

import org.gridlab.gat.GAT;

public class JavaGATFilesInterface extends FilesInterface {

    @Override
    public File createFile(String path) throws Exception {
	return GAT.createFile(path);
    }

    @Override
    public InputStream createInputStream(File file) throws Exception {
	return GAT.createFileInputStream((org.gridlab.gat.io.File) file);
    }

    @Override
    public OutputStream createOutputStream(File file) throws Exception {
	return GAT.createFileOutputStream((org.gridlab.gat.io.File) file);
    }

    @Override
    public TripleFile[] getListFiles(Configuration conf, String path,
	    boolean sort) {

	File dir = null;
	try {
	    dir = createFile(path);
	} catch (Exception e1) {
	    // TODO deal with this.

	}
	if (!dir.exists()) {
	    return new TripleFile[0];
	}

	Constructor<? extends TripleFile> constructor = Utils
		.getTripleFileImplementationWithFi(conf);
	ArrayList<TripleFile> list = new ArrayList<TripleFile>();
	for (File file : dir.listFiles()) {
	    String fileName = file.getName();
	    if (!fileName.startsWith("_") && !fileName.startsWith(".")) {
		try {
		    TripleFile newFile = constructor.newInstance(
			    ((org.gridlab.gat.io.File) file).toGATURI()
				    .toString(), this);
		    list.add(newFile);
		} catch (Exception e) {
		    System.out.println("Exception");
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

    @Override
    public String getFilesSeparator() {
	return org.gridlab.gat.io.File.separator;
    }

    @Override
    protected RandomAccessFile createRandomAccessFile(File f) throws Exception {
	return GAT.createRandomAccessFile(
		((org.gridlab.gat.io.File) f).toGATURI(), "rw");
    }

    @Override
    protected void copyTo(File source, File dest) throws Exception {
	((org.gridlab.gat.io.File) source)
		.copy(((org.gridlab.gat.io.File) dest).toGATURI());
    }
}