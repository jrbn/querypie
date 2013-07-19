package nl.vu.cs.querypie.storage.disk;

public abstract class TripleFile {

	protected String pathFile = null;

	public TripleFile(String pathFile) {
		this.pathFile = pathFile;
	}

	public abstract String getPath();

	public abstract String getName();

	public abstract boolean next();

	public abstract byte[] getTriple();

	public abstract long getFirstTerm();

	public abstract long getSecondTerm();

	public abstract long getThirdTerm();

	public abstract void open();

	public abstract void openToWrite();

	public abstract void write(long firstTerm, long secondTerm, long thirdTerm);

	public abstract void writeTriple(byte[] triple, int length);

	public abstract void close();

	public abstract void renameTo(String newFilename);

	public abstract void delete();

	public abstract void copyTo(TripleFile newFile);

	public abstract boolean exists();
}