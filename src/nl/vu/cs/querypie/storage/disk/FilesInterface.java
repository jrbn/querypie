package nl.vu.cs.querypie.storage.disk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import nl.vu.cs.querypie.utils.Utils;

import org.apache.commons.io.FileUtils;

import arch.utils.Configuration;

public class FilesInterface {

    public File createFile(String path) throws Exception {
	return new File(path);
    }

    public InputStream createInputStream(File file) throws Exception {
	return new FileInputStream(file);
    }

    public OutputStream createOutputStream(File file) throws Exception {
	return new FileOutputStream(file);
    }

    public TripleFile[] getListFiles(Configuration conf, String path,
	    boolean flag) {
	return Utils.getListFiles(conf, path, flag);
    }

    public String getFilesSeparator() {
	return File.separator;
    }

    protected RandomAccessFile createRandomAccessFile(File f) throws Exception {
	return new RandomAccessFile(f, "rw");
    }

    protected void copyTo(File source, File dest) throws Exception {
	FileUtils.copyFile(source, dest);
    }
}