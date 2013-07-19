package arch.buckets;

import ibis.util.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.StatisticsCollector;
import arch.chains.ChainNotifier;
import arch.data.types.Tuple;
import arch.data.types.bytearray.FDataInput;
import arch.data.types.bytearray.FDataOutput;
import arch.datalayer.TupleIterator;
import arch.storage.Factory;
import arch.storage.RawComparator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

import com.hadoop.compression.lzo.LzoCodec;

public class Bucket {

	public static class FileMetaData {
		String filename;
		FDataInput stream;
		byte[] lastElement;
		long nElements;
		long remainingSize;
	}

	static class SortedList<T> extends ArrayList<T> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 919780998989097240L;
		private final Comparator<T> comparator;

		public SortedList(int capacity, Comparator<T> comparator) {
			super(capacity);
			this.comparator = comparator;
		}

		/**
		 * Adds this element to the list at the proper sorting position.
		 */
		@Override
		public synchronized boolean add(T e) {
			if (size() == 0) {
				return super.add(e);
			}

			int index = Collections.binarySearch(this, e, comparator);
			if (index < 0) {
				index = -index - 1;
			}
			super.add(index, e);
			return true;
		}
	}

	static final Logger log = LoggerFactory.getLogger(Bucket.class);

	private long key;
	private WritableContainer<Tuple> tuples = null;

	private int nChainsReceived = 0;
	private int nBucketReceived = 0;

	private final byte[] sequencesReceived = new byte[Consts.MAX_SEGMENTS_RECEIVED];
	private int highestSequence;
	private final Map<Long, int[]> childrens = new HashMap<Long, int[]>();
	private boolean isFinished;

	private int rootChainsReplication = 0;
	private boolean receivedMainChain;

	private int submissionNode;
	private int submissionId;
	private StatisticsCollector stats;

	boolean gettingData;

	private int elementsInCache = 0;
	Factory<WritableContainer<Tuple>> fb = null;

	CachedFilesMerger merger;
	LzoCodec codec = null;

	// Used for not sorted streams
	private List<FDataInput> cacheFiles = new ArrayList<FDataInput>();
	Map<byte[], FileMetaData> sortedCacheFiles = new HashMap<byte[], FileMetaData>();
	SortedList<byte[]> minimumSortedList = new SortedList<byte[]>(100,
			new Comparator<byte[]>() {
				@Override
				public int compare(byte[] o1, byte[] o2) {
					return -comparator.compare(o1, 0, o1.length, o2, 0,
							o2.length);
				}
			});
	RawComparator<Tuple> comparator = null;
	private boolean isBufferSorted = true;

	public void init(long key, StatisticsCollector stats, int submissionNode,
			int submissionId, String comparator, Object[] params,
			Factory<WritableContainer<Tuple>> fb, CachedFilesMerger merger) {
		if (codec == null) {
			codec = new LzoCodec();
			codec.setConf(new Configuration());
		}
		this.key = key;
		this.fb = fb;
		this.tuples = null;
		this.merger = merger;

		isFinished = false;
		receivedMainChain = false;
		rootChainsReplication = 0;
		nChainsReceived = 0;
		nBucketReceived = 0;
		highestSequence = -1;
		gettingData = false;
		
		notifier = null;
		iter = null;

		elementsInCache = 0;

		sortedCacheFiles.clear();
		minimumSortedList.clear();
		cacheFiles.clear();
		childrens.clear();

		this.stats = stats;
		this.submissionNode = submissionNode;
		this.submissionId = submissionId;

		isBufferSorted = true;
		if (comparator != null && comparator.length() > 0) {
			setSortingFunction(comparator, params);
		} else {
			this.comparator = null;
		}
	}

	public long getKey() {
		return key;
	}

	public synchronized void updateCounters(long idChain, long idParentChain,
			int children, int replicatedFactor, boolean isResponsible)
			throws IOException {

		if (log.isDebugEnabled()) {
			log.debug("Update counters of bucket " + this.key + ": ic "
					+ idChain + " p " + idParentChain + " c " + children
					+ " r " + replicatedFactor + " " + isResponsible);
		}

		if (children > 0) { // Set the expected children in the
			// map
			int[] c = childrens.get(idChain);
			if (c == null) {
				c = new int[2];
				childrens.put(idChain, c);
			}
			c[0] += children;
			if (c[0] == 0 && c[0] == c[1])
				childrens.remove(idChain);
		}

		if (isResponsible) { // It is a root chain
			rootChainsReplication += replicatedFactor - 1;
			receivedMainChain = true;
		} else {
			int[] c = childrens.get(idParentChain);
			if (c == null) {
				c = new int[2];
				childrens.put(idParentChain, c);
			}

			if (replicatedFactor > 0) { // It is an original chain
				c[0]--;
			}
			c[1] += replicatedFactor - 1;
			if (c[0] == 0 && c[0] == c[1])
				childrens.remove(idParentChain);
		}
		/*
		 * if (log.isDebugEnabled() && childrens.size() > 0) { for
		 * (Map.Entry<Long, int[]> e : childrens.entrySet()) {
		 * log.debug("Children " + e.getKey() + " [ " + e.getValue()[0] + ", " +
		 * e.getValue()[1] + "]"); } }
		 */

		// if (replicatedFactor > 0) {
		nChainsReceived++;
		// }

		checkFinished();
	}

	public synchronized void updateCounters(int sequence, boolean lastSequence)
			throws IOException {

		if (log.isDebugEnabled()) {
			log.debug("updateCounters of bucket: " + this.key + ", sequence = "
					+ sequence + ", lastSequence = " + lastSequence);
		}

		sequencesReceived[sequence]++;
		if (highestSequence < sequence)
			highestSequence = sequence;
		if (lastSequence) {
			nBucketReceived++;
			for (int i = 0; i < sequence + 1; ++i) {
				sequencesReceived[i]--;
			}
		}
		checkFinished();
	}

	private void checkFinished() throws IOException {

		/*
		 * if (log.isDebugEnabled()) { log.debug("checkFinished of bucket: " +
		 * this.key + ", nChainsReceived = " + nChainsReceived +
		 * ", nBucketReceived = " + nBucketReceived + ", highestSequence = " +
		 * highestSequence + ", rootChainsReplication = " +
		 * rootChainsReplication + ", childrens.size() = " + childrens.size() +
		 * ", receivedMainChain = " + receivedMainChain); }
		 */
		if (nChainsReceived == nBucketReceived && highestSequence != -1
				&& rootChainsReplication == 0 && childrens.size() == 0
				&& receivedMainChain) {
			for (int i = 0; i < highestSequence + 1; ++i)
				if (sequencesReceived[i] != 0) {
					return;
				}
			if (log.isDebugEnabled()) {
				log.debug("Calling setFinished on bucket " + this.key);
			}
			setFinished(true);
		}
	}

	public synchronized void addAll(
			WritableContainer<Tuple> newTuplesContainer, boolean isSorted,
			Factory<WritableContainer<Tuple>> factory) throws Exception {
		if (tuples == null) {
			tuples = fb.get();
			tuples.clear();
		}

		// If factory is not null, we get control over the newTuplesContainer,
		// which means that we have to remove it
		//
		if (comparator != null && !isSorted) {
			throw new Exception("This buffer accepts only presorted sets");
		}

		boolean isBufferEmpty = tuples.getNElements() == 0;

		// TODO: if isBufferEmpty, could we just release tuples and replace it
		// with newTuplesContainer?
		// Also, if tuples has fewer elements, could we just addAll tuples to
		// newTuplesContainer,
		// and then release tuples, and replace it with newTuplesContainer?
		boolean response = tuples.addAll(newTuplesContainer);
		isBufferSorted = (response && isBufferEmpty)
				|| (isBufferSorted && !response);
                if (tuples.getNElements() > 10000000) {
                    log.warn("Adding a bucket with " + newTuplesContainer.getNElements() + " to a bucket with " + tuples.getNElements() + " elements");
                }
		if (!response) {

			if (tuples.getNElements() > newTuplesContainer.getNElements()) {
				cacheCurrentBuffer();
				response = tuples.addAll(newTuplesContainer);
				isBufferSorted = true;
//				if (factory != null) {
//					factory.release(newTuplesContainer);
//				}
				if (!response) {
					// The tuples are bigger than the entire buffer. Must throw
					// exception
					throw new Exception(
							"The buffer is too big! Must increase the buffer size.");
				}
			} else {
				// It's better to store the other buffer
				if (factory != null) {
					// We can just use the other container
					cacheBuffer(newTuplesContainer, isSorted, factory);
				} else {
					// Copy the container ...
					WritableContainer<Tuple> box = fb.get();
					newTuplesContainer.copyTo(box);
					cacheBuffer(box, isSorted, fb);
				}
			}
//		} else if (factory != null) {
//			factory.release(newTuplesContainer);
		}
	}

	public synchronized void copyTo(Bucket bucket) throws Exception {

		if (tuples == null) {
			tuples = fb.get();
			tuples.clear();
		}

		waitForCachers();

		if (tuples.getNElements() > 0) {

			if (comparator != null && !isBufferSorted) {
				tuples.sort(comparator, fb);
				isBufferSorted = true;
			}

			bucket.addAll(tuples, isBufferSorted, null);
		}

		if (elementsInCache > 0) {
			// There are some files to move in the new buffer
			synchronized (bucket) {
				if (comparator == null) {
					bucket.cacheFiles.addAll(cacheFiles);
				} else {
					bucket.sortedCacheFiles.putAll(sortedCacheFiles);
					for (byte[] min : minimumSortedList) {
						bucket.minimumSortedList.add(min);
					}
				}
				bucket.elementsInCache += elementsInCache;
			}
		}
	}

	public synchronized boolean add(Tuple tuple) throws Exception {

		if (tuples == null) {
			tuples = fb.get();
			tuples.clear();
		}

		boolean response = tuples.add(tuple);
		if (response) {
			isBufferSorted = tuples.getNElements() < 2;
		} else {
			cacheCurrentBuffer();
			response = tuples.add(tuple);
			isBufferSorted = true;

			if (!response) {
				throw new Exception(
						"The buffer is too small! Must increase the buffer size.");
			}
		}

		return response;
	}

	public synchronized void setFinished(boolean value) throws IOException {
		isFinished = value;
		if (isFinished && notifier != null) {
		    notifier.markReady(iter);
		    notifier = null;
		    iter = null;
		}
		notifyAll();
	}

	public synchronized boolean isFinished() {
		return isFinished;
	}

	public synchronized boolean waitUntilFinished() {
		try {
			while (!isFinished) {
				if (log.isDebugEnabled()) {
					log.debug("waitUntilFinished on bucket " + this.key);
				}
				wait();
				if (log.isDebugEnabled()) {
					log.debug("waitUntilFinished on bucket " + this.key
							+ " done");
				}
			}
		} catch (Exception e) {
			// ignore
		}

		return true;
	}

	public synchronized boolean removeChunk(WritableContainer<Tuple> tmpBuffer) {

		gettingData = true;

		// If some threads still have to finish writing
		waitForCachers();

		try {
			if (comparator != null && !isBufferSorted) {
				tuples.sort(comparator, fb);
				isBufferSorted = true;
			}

			if (elementsInCache > 0) {
				if (comparator == null) { // No sorting applied
					long time = System.currentTimeMillis();
					FDataInput di = cacheFiles.remove(0);
					tmpBuffer.readFrom(di); // Read the oldest file
					stats.addCounter(submissionNode, submissionId,
							"Time spent reading from cache (ms)",
							System.currentTimeMillis() - time);
					stats.addCounter(submissionNode, submissionId,
							"Bytes read from cache", tmpBuffer.bytesToStore());
					elementsInCache -= tmpBuffer.getNElements();
					di.close();
				} else { // Need to sort

					// Add the first triple from the in-memory ds to the pool
					if (tuples.getNElements() > 0
							&& minimumSortedList.size() == sortedCacheFiles
									.size()) {
						byte[] key = tuples.removeRaw(null);
						minimumSortedList.add(key);
					}

					if (log.isDebugEnabled()) {
						log.debug("Sorting bucket: number of streams is "
								+ sortedCacheFiles.size());
					}

					boolean insertResponse = false;
					int tuplesFromBuffer = 0;
					int tuplesFromStream = 0;
					long time = System.currentTimeMillis();
					do {
						// Remove the minimum of the tuples
						byte[] minimum = minimumSortedList
								.remove(minimumSortedList.size() - 1);

						// Add it to the buffer
						insertResponse = tmpBuffer.addRaw(minimum);

						if (insertResponse) {
							// If the stream comes from a stream replace it
							if (sortedCacheFiles.containsKey(minimum)) {
								tuplesFromStream++;
								elementsInCache--;

								FileMetaData meta = sortedCacheFiles
										.get(minimum);

								// Check whether the last element is smaller
								// than the second minimum. If it is, then we
								// can copy the entire file in the buffer.
								// Ceriel: minimumSortedList can be empty here
								// ...
								if (minimumSortedList.size() == 0
										|| (meta.lastElement != null && minimumSortedList.comparator
												.compare(
														meta.lastElement,
														minimumSortedList
																.get(minimumSortedList
																		.size() - 1)) >= 0)) {
									// Copy the entire file in the buffer
									if (tmpBuffer.addAll(meta.stream,
											meta.lastElement, meta.nElements,
											meta.remainingSize)) {
										elementsInCache -= meta.nElements;
										meta.stream.close();
										sortedCacheFiles.remove(minimum);
										return true;
									}
								}

								try {
									int length = meta.stream.readInt();
									if (length == minimum.length) {
										// Reuse minimum
										meta.stream.readFully(minimum);
										meta.remainingSize -= length + 4;
										meta.nElements--;

										for (;;) {
											// Create a "fast path": sometimes,
											// a single cache file has
											// tuples that are all smaller than
											// the next file.
											if (minimumSortedList.size() == 0
													|| minimumSortedList.comparator
															.compare(
																	minimum,
																	minimumSortedList
																			.get(minimumSortedList
																					.size() - 1)) >= 0) {
												// Add it to the buffer
												insertResponse = tmpBuffer
														.addRaw(minimum);
												if (!insertResponse) {
													minimumSortedList
															.add(minimum);
													break;
												}
												tuplesFromStream++;
												elementsInCache--;
												length = meta.stream.readInt();
												// FIXME this if is not correct!
												if (length != minimum.length) {
													break;
												}
												meta.stream.readFully(minimum);
												meta.remainingSize -= length + 4;
												meta.nElements--;
											} else {
												minimumSortedList.add(minimum);
												break;
											}
										}
									} else if (length != minimum.length
											&& length > 0) {
										log.warn("The buffer is resized! New length="
												+ length);
										// Replace it
										sortedCacheFiles.remove(minimum);
										byte[] rawValue = new byte[length];
										meta.stream.readFully(rawValue);
										meta.remainingSize -= length + 4;
										meta.nElements--;

										minimumSortedList.add(rawValue);
										sortedCacheFiles.put(rawValue, meta);
									} else if (length <= 0) { // File is
										// finished
										sortedCacheFiles.remove(minimum);
										meta.stream.close();
									}
								} catch (Exception e) {
									log.warn("Here it should never come!");
									sortedCacheFiles.remove(minimum);
									meta.stream.close();
								}

							} else { // It was coming from inmemory container.
								if (tuples.getNElements() > 0
										&& elementsInCache > 0) {
									byte[] key = tuples.removeRaw(minimum);
									minimumSortedList.add(key);
								}
								tuplesFromBuffer++;
							}
						} else {
							// Put it back
							minimumSortedList.add(minimum);
						}
					} while (insertResponse && elementsInCache > 0);

					if (log.isDebugEnabled()) {
						log.debug("Tuples from buffer:" + tuplesFromBuffer
								+ " from stream: " + tuplesFromStream
								+ " time: "
								+ (System.currentTimeMillis() - time));
					}
				}
			}

			if (elementsInCache == 0) {
				if (minimumSortedList.size() > 0) {
					// There cannot be more than 1 here
					byte[] minimum = minimumSortedList.remove(minimumSortedList
							.size() - 1);
					if (!tmpBuffer.addRaw(minimum)) {
						minimumSortedList.add(minimum);
					}
				}

				if (tuples != null && tuples.getNElements() > 0) {
					boolean response = tmpBuffer.addAll(tuples);
					if (response) {
						tuples.clear();
					}
				}
			}

		} catch (Exception e) {
			log.error("Error in retrieving the results", e);
		}

		return isFinished && elementsInCache == 0
				&& (tuples == null || tuples.getNElements() == 0)
				&& (sortedCacheFiles == null || minimumSortedList.size() == 0);
	}

	public synchronized boolean availableToTransmit() {
		return elementsInCache > 0
				|| (tuples != null && tuples.bytesToStore() > Consts.MIN_SIZE_TO_SEND);
	}

	int numCachers;
	int numWaitingForCachers;

    private ChainNotifier notifier;

    private TupleIterator iter;

	private void cacheCurrentBuffer() throws IOException {

		if (tuples.getNElements() > 0) {
                        log.warn("Caching buffer, tuples.getNElements = " + tuples.getNElements(), new Throwable());
			if (log.isDebugEnabled()) {
				log.debug("Caching buffer");
			}
			cacheBuffer(tuples, isBufferSorted, fb);
			tuples = fb.get();
			tuples.clear();
		}
	}

	private void cacheBuffer(final WritableContainer<Tuple> buffer,
			final boolean sorted, final Factory<WritableContainer<Tuple>> fb)
			throws IOException {

		if (buffer.getNElements() == 0) {
			// nothing to cache.
			return;
		}

		synchronized (this) {
			elementsInCache += buffer.getNElements();
			numCachers++;
		}

		ThreadPool.createNew(new Runnable() {
			@Override
			public void run() {
				try {
					if (comparator != null && !sorted) {
						buffer.sort(comparator, fb);
					}

					File cacheFile = File.createTempFile("cache", "tmp");
					cacheFile.deleteOnExit();

					BufferedOutputStream fout = new BufferedOutputStream(
							codec.createOutputStream(new FileOutputStream(
									cacheFile)));
					FDataOutput cacheOutputStream = new FDataOutput(fout);

					long time = System.currentTimeMillis();
					if (comparator == null) {
						buffer.writeTo(cacheOutputStream);
					} else {
						buffer.writeElementsTo(cacheOutputStream);
					}

					stats.addCounter(submissionNode, submissionId,
							"Time spent writing to cache (ms)",
							System.currentTimeMillis() - time);

					cacheOutputStream.close();

					// Register file in the list of cachedBuffers
					FDataInput is = new FDataInput(new BufferedInputStream(
							codec.createInputStream(new FileInputStream(
									cacheFile)), 65536));

					synchronized (Bucket.this) {
						if (comparator == null) {
							cacheFiles.add(is);
						} else {
							// Read the first element and put it into the map
							try {
								int length = is.readInt();
								byte[] rawValue = new byte[length];
								is.readFully(rawValue);

								FileMetaData meta = new FileMetaData();
								meta.filename = cacheFile.getAbsolutePath();
								meta.stream = is;
								meta.nElements = buffer.getNElements() - 1;
								meta.lastElement = buffer.returnLastElement();
								meta.remainingSize = buffer
										.getRawElementsSize() - 4 - length;

								sortedCacheFiles.put(rawValue, meta);
								minimumSortedList.add(rawValue);

							} catch (Exception e) {
								log.error("Error", e);
							}

							if (sortedCacheFiles.size() > 8) {
								merger.newRequest(Bucket.this);
							}
						}
					}
// 					fb.release(buffer);
				} catch (IOException e) {
					// TODO: what to do now?
					log.error("Got exception while writing cache!", e);
				}
				synchronized (Bucket.this) {
					numCachers--;
					if (numCachers == 0 && numWaitingForCachers > 0) {
						Bucket.this.notifyAll();
					}
				}
			}
		}, "Sort-and-cache");

	}

	private synchronized void waitForCachers() {
		numWaitingForCachers++;
		while (numCachers > 0) {
			if (log.isDebugEnabled()) {
				log.debug("Waiting for cachers: " + numCachers);
			}
			try {
				wait();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		numWaitingForCachers--;
	}

	@SuppressWarnings("unchecked")
	public void setSortingFunction(String sortingFunction, Object[] params) {
		try {
			this.comparator = (RawComparator<Tuple>) Class.forName(
					sortingFunction).newInstance();
			comparator.init(params);
		} catch (Exception e) {
			log.error("Error instantiating the comparator.", e);
		}
	}

	public RawComparator<Tuple> getSortingFunction() {
		return comparator;
	}

	public void releaseTuples() {
		if (tuples != null) {
// 			fb.release(tuples);
			tuples = null;
		}
	}

	public boolean isEmpty() {
		return elementsInCache == 0
				&& (tuples == null || tuples.getNElements() == 0);
	}

	public long inmemory_size() {
		if (tuples == null) {
			return 0;
		} else {
			return tuples.inmemory_size();
		}
	
	}

    public synchronized void registerFinishedNotifier(ChainNotifier notifier, TupleIterator iter) {
        if (isFinished()) {
            notifier.markReady(iter);
            return;
        }
        this.notifier = notifier;
        this.iter = iter;
    }
}