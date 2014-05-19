package nl.vu.cs.querypie.storage;

public interface TripleIterator {
	public long estimateRecords() throws Exception;
	public void stopReading();
}
