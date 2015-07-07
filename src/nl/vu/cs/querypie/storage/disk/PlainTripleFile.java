package nl.vu.cs.querypie.storage.disk;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import nl.vu.cs.querypie.utils.Utils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainTripleFile extends TripleFile {

	static final Logger log = LoggerFactory.getLogger(PlainTripleFile.class);

	boolean hasNext = false;
	final byte[] triple = new byte[24];
	// private final byte[] value = new byte[8];
	// private final ByteBuffer bvalue = ByteBuffer.wrap(value);

	protected File file = null;

	FileInputStream fin = null;
	GZIPInputStream gin = null;
	protected DataInputStream din = null;

	protected GZIPOutputStream fon = null;
	FileOutputStream fileOutput = null;

	@Override
	public String toString() {
		return getName();
	}

	FilesInterface fi;

	public PlainTripleFile(String pathFile, FilesInterface fi) {
		super(pathFile);
		try {
			this.fi = fi;
			this.file = fi.createFile(pathFile);
		} catch (Exception e) {
			log.error("Failed creating the file", e);
		}
	}

	public PlainTripleFile(String pathFile) {
		this(pathFile, new FilesInterface());
	}

	@Override
	public String getPath() {
		return file.getAbsolutePath();
	}

	@Override
	public String getName() {
		return file.getName();
	}

	@Override
	public boolean next() {
		try {
			din.readFully(triple);
			return true;
		} catch (EOFException e1) {
		} catch (Exception e) {
			log.error("Error accessing file " + pathFile, e);
		}
		return false;
	}

	@Override
	public byte[] getTriple() {
		return triple;
	}

	@Override
	public long getFirstTerm() {
		return Utils.decodeLong(triple, 0);
	}

	@Override
	public long getSecondTerm() {
		return Utils.decodeLong(triple, 8);
	}

	@Override
	public long getThirdTerm() {
		return Utils.decodeLong(triple, 16);
	}

	protected File createFile(String name) {
		return new File(name);
	}

	@Override
	public void open() {
		try {
			din = new DataInputStream(new GZIPInputStream(
					new BufferedInputStream(fi.createInputStream(file),
							64 * 1024)));
		} catch (Exception e) {
			log.error("Error", e);
		}
	}

	@Override
	public void openToWrite() {
		try {
			file.createNewFile();
			fon = new GZIPOutputStream(new BufferedOutputStream(
					fi.createOutputStream(file), 64 * 1024));
		} catch (Exception e) {
			log.error("Error", e);
		}
	}

	@Override
	public void write(long firstTerm, long secondTerm, long thirdTerm) {
		try {
			// byte[] value = new byte[8];
			Utils.encodeLong(triple, 0, firstTerm);
			// fon.write(value);
			Utils.encodeLong(triple, 8, secondTerm);
			// fon.write(value);
			Utils.encodeLong(triple, 16, thirdTerm);
			fon.write(triple);
		} catch (Exception e) {
			log.error("Error", e);
		}
	}

	@Override
	public void writeTriple(byte[] triple, int length) {
		try {
			fon.write(triple, 0, length);
		} catch (Exception e) {
			log.error("Error ", e);
		}
	}

	@Override
	public void close() {
		try {
			if (din != null) {
				din.close();
			}

			if (fon != null) {
				fon.close();
			}

			din = null;
			fon = null;
		} catch (Exception e) {
			log.error("Error", e);
		}
	}

	@Override
	public void renameTo(String newFilename) {
		try {
			String orig = newFilename;
			int i = 1;
			while (new File(orig).exists()) {
				orig = newFilename + "." + i++;
			}

			file.renameTo(fi.createFile(orig));
			pathFile = orig;
			file = fi.createFile(pathFile);
		} catch (Exception e) {
			log.error("Error renaming files", e);
		}
	}

	@Override
	public void delete() {
		file.delete();
	}

	@Override
	public boolean exists() {
		return file.exists();
	}

	@Override
	public void copyTo(TripleFile newFile) {
		// Copy all the content of the file to the new file
		if (newFile instanceof PlainTripleFile) {
			try {
				PlainTripleFile file = (PlainTripleFile) newFile;
				IOUtils.copy(din, file.fon);
			} catch (Exception e) {
				log.error("Error", e);
			}
		}
	}

}
