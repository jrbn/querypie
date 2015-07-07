package nl.vu.cs.querypie.storage.memory;

import ibis.util.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import nl.vu.cs.querypie.QueryPIE;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.storage.RDFTerm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleSet implements java.io.Serializable {

	private static final long serialVersionUID = 3919204682759914381L;

	static final Logger log = LoggerFactory.getLogger(TupleSet.class);

	public static void concatenateTuples(long[] newTuple, long[] first,
			int pos1, int off1, long[] second, int pos2, int[] pos_sec) {

		// long[] newTuple = new long[off1 + pos_sec.length];
		System.arraycopy(first, pos1, newTuple, 0, off1);

		for (int i = 0; i < pos_sec.length; ++i) {
			newTuple[off1 + i] = second[pos2 + pos_sec[i]];
		}

		// return newTuple;
	}

	public static long[] concatenateTuples(long[] first, int pos1, int off1,
			long[] second, int pos2, int[] pos_sec) {

		long[] newTuple = new long[off1 + pos_sec.length];
		System.arraycopy(first, pos1, newTuple, 0, off1);

		for (int i = 0; i < pos_sec.length; ++i) {
			newTuple[off1 + i] = second[pos2 + pos_sec[i]];
		}

		return newTuple;
	}

	public static long[] concatenateTuples(long[] first, int pos1, int off1,
			long[] second) {
		long[] newTuple = new long[off1 + second.length];
		System.arraycopy(first, pos1, newTuple, 0, off1);
		System.arraycopy(second, 0, newTuple, off1, second.length);
		return newTuple;
	}

	public static List<String> concatenateBindings(List<String> bindings,
			Pattern pattern) {
		// Concatenate the names and add a new system
		List<String> newNameBindings = new ArrayList<String>();
		Set<String> existingBindings = new HashSet<String>();

		existingBindings.addAll(bindings);
		newNameBindings.addAll(bindings);
		List<Integer> positionsToAdd = new ArrayList<Integer>();
		for (int i = 0; i < pattern.p.length; ++i) {
			RDFTerm t = pattern.p[i];
			if (t.getName() != null && !existingBindings.contains(t.getName())) {
				newNameBindings.add(t.getName());
				existingBindings.add(t.getName());
				positionsToAdd.add(i);
			}
		}

		return newNameBindings;
	}

	private List<String> nameBindings = null;
	int nTuples = 0;
	int sizeTuple = 0;
	int currentSize = 0;

	int threshold = 8;
	private long[] bindings = new long[12];

	private transient Map<String, Object> cache = new HashMap<String, Object>();

	public TupleSet() {
		nameBindings = new ArrayList<String>();
	}

	public TupleSet(List<String> nameBindings) {
		this.nameBindings = nameBindings;
		sizeTuple = nameBindings.size();
	}

	public TupleSet(List<String> newBindings, List<long[]> results) {
		this.nameBindings = newBindings;
		sizeTuple = newBindings.size();
		nTuples = results.size();
		currentSize = sizeTuple * nTuples;
		bindings = new long[currentSize];
		threshold = -1;

		int i = 0;
		for (long[] tuple : results) {
			for (long v : tuple)
				bindings[i++] = v;
		}
	}

	public TupleSet(List<String> newBindings, long[] bindings) {
		this.nameBindings = newBindings;
		sizeTuple = newBindings.size();
		nTuples = bindings.length / sizeTuple;
		currentSize = bindings.length;
		this.bindings = bindings;
		threshold = -1;
	}

	public Mapping[] calculateSharedVariables(Pattern pattern) {
		List<Mapping> list = new ArrayList<Mapping>();

		for (int i = 0; i < pattern.p.length; ++i) {
			RDFTerm t = pattern.p[i];
			for (int j = 0; j < nameBindings.size(); ++j) {
				String name = nameBindings.get(j);
				if (t.getName() != null && t.getName().equals(name)) {
					Mapping map = new Mapping();
					map.nameBinding = name;
					map.pos1 = j;
					map.pos2 = i;
					list.add(map);
				}
			}
		}

		return list.toArray(new Mapping[list.size()]);
	}

	public void addTuple(long[] tuple) {
		if ((currentSize + sizeTuple) > threshold) { // Enlarge it by 30%
			long[] newSize = new long[bindings.length + bindings.length];
			System.arraycopy(bindings, 0, newSize, 0, currentSize);
			threshold = newSize.length * 2 / 3;
			bindings = newSize;
		}

		System.arraycopy(tuple, 0, bindings, currentSize, sizeTuple);
		currentSize += sizeTuple;
		nTuples++;

	}

	public List<String> concatenateBindings(Pattern p) {
		return concatenateBindings(nameBindings, p);
	}

	public int size() {
		return nTuples;
	}

	public List<String> getNameBindings() {
		return nameBindings;
	}

	private synchronized Collection<Long> getAllValuesNoCache(int pos)
			throws Exception {
		Collection<Long> set = new TreeSet<Long>();
		for (int x = 0; x < currentSize; x += sizeTuple) {
			set.add(bindings[x + pos]);
		}
		return set;
	}

	public synchronized Collection<Long> getAllValues(String binding,
			boolean compress) throws Exception {
		int pos = 0;
		for (String var : nameBindings) {
			if (var.equals(binding)) {
				break;
			}
			pos++;
		}
		return getAllValues(pos, compress);
	}

	public synchronized Collection<Long> getAllValues(int pos, boolean compress)
			throws Exception {
		String key = Integer.toString(pos);

		@SuppressWarnings("unchecked")
		Collection<Long> col = (Collection<Long>) cache.get(key);
		if (col == null) {
			col = getAllValuesNoCache(pos);
			if (compress)
				col = new SortedCollectionTuples(col);
			cache.put(key, col);
		}
		return col;
	}

	public synchronized SortedCollectionTuples getAllValues(int[] pos)
			throws Exception {
		String key = Arrays.toString(pos);
		SortedCollectionTuples col = (SortedCollectionTuples) cache.get(key);
		if (col == null) {
			Set<MultiValue> tmpSet = new TreeSet<MultiValue>(
					new MultiValue.Comparator());
			int l = pos.length;
			for (int x = 0; x < currentSize; x += sizeTuple) {
				long[] b = new long[l];
				for (int j = 0; j < l; ++j)
					b[j] = bindings[x + pos[j]];
				MultiValue v = new MultiValue(b);
				tmpSet.add(v);
			}
			col = new SortedCollectionTuples(tmpSet);
			cache.put(key, col);
		}
		return col;
	}

	@SuppressWarnings("unchecked")
	public Collection<Long>[] getAllValues(int[] posSharedVars, boolean compress)
			throws Exception {
		if (compress) {
			throw new Exception("Not implemented");
		}
		Collection<Long>[] output = new Collection[posSharedVars.length];
		for (int i = 0; i < output.length; ++i) {
			output[i] = new TreeSet<Long>();
		}

		for (int x = 0; x < currentSize; x += sizeTuple) {
			for (int j = 0; j < posSharedVars.length; ++j)
				output[j].add(bindings[x + posSharedVars[j]]);
		}
		return output;
	}

	public synchronized SortedCollectionTuples getAllValues() {
		String key = "all";
		SortedCollectionTuples col = (SortedCollectionTuples) cache.get(key);

		if (col == null) {

			Set<MultiValue> tmpSet = new TreeSet<MultiValue>(
					new MultiValue.Comparator());
			for (int x = 0; x < currentSize; x += sizeTuple) {
				MultiValue v = new MultiValue(bindings, x, sizeTuple);
				tmpSet.add(v);
			}
			col = new SortedCollectionTuples(tmpSet);

			cache.put(key, col);

		}
		return col;
	}

	public synchronized TupleMap getBindingsFromBindings(int[] inputBinding,
			int[] outputBinding) {
		return getBindingsFromBindings(inputBinding, outputBinding, false);
	}

	public synchronized TupleMap getBindingsFromBindings(int[] inputBinding,
			int[] outputBinding, boolean compression) {

		String skey = Arrays.toString(inputBinding) + "-"
				+ Arrays.toString(outputBinding);
		TupleMap map = (TupleMap) cache.get(skey);

		try {

			if (map == null) {
				Map<MultiValue, Collection<long[]>> tmpMap = new HashMap<MultiValue, Collection<long[]>>();
				for (int x = 0; x < currentSize; x += sizeTuple) {
					long[] key = new long[inputBinding.length];
					for (int i = 0; i < inputBinding.length; ++i) {
						key[i] = bindings[x + inputBinding[i]];
					}

					MultiValue mk = new MultiValue(key);
					Collection<long[]> set = tmpMap.get(mk);
					if (set == null) {
						set = new ArrayList<long[]>();
						tmpMap.put(mk, set);
					}

					if (outputBinding == null) {
						long[] tuple = new long[sizeTuple];
						System.arraycopy(bindings, x, tuple, 0, sizeTuple);
						set.add(tuple);
					} else {
						long[] v = new long[outputBinding.length];
						for (int i = 0; i < v.length; ++i) {
							v[i] = bindings[x + outputBinding[i]];
						}
						set.add(v);
					}
				}

				if (inputBinding.length == 1)
					map = new SingleTupleMap();
				else
					map = new MultipleTupleMap();

				int s = -1;
				if (outputBinding == null) {
					s = sizeTuple;
				} else {
					s = outputBinding.length;
				}
				for (Map.Entry<MultiValue, Collection<long[]>> entry : tmpMap
						.entrySet()) {
					long[] values = new long[s * entry.getValue().size()];
					int sv = 0;
					for (long[] v : entry.getValue()) {
						for (int n = 0; n < v.length; ++n) {
							values[sv++] = v[n];
						}
					}
					CollectionTuples col = new CollectionTuples(s, values);
					map.put(entry.getKey(), col);
				}

				if (inputBinding.length == 1 && compression) {
					log.debug("Compressing the bindings list ...");
					map = new ComprSingleTupleMap((SingleTupleMap) map);
					log.debug("End compression.");
				}

				cache.put(skey, map);
			}

		} catch (Exception e) {
			log.error("Error", e);
		}
		return map;
	}

	public static TupleSet join2(List<Pattern> otherPatterns,
			List<long[]> triples, String[] destination, int pos_pattern)
					throws Exception {

		List<String> completeList = new ArrayList<String>();
		final List<Mapping[]> positionsToJoin = new ArrayList<Mapping[]>();
		final List<int[]> positionsToCopy = new ArrayList<int[]>();
		final List<TupleMap> mappings = new ArrayList<TupleMap>();

		// Prepare the join orderings
		for (int i = 0; i < otherPatterns.size(); ++i) {

			// Extract the variables of the pattern
			Pattern pattern = otherPatterns.get(i);
			int[] pos_vars = pattern.getPositionVars();
			List<String> name_bindings = new ArrayList<String>();
			for (int pos : pos_vars) {
				name_bindings.add(pattern.p[pos].getName());
			}

			if (i == 0) {
				completeList.addAll(name_bindings);
			} else {

				// Calculate positions shared variables and put them in list
				// mappings
				List<Mapping> list = new ArrayList<Mapping>();
				for (int j = 0; j < completeList.size(); ++j) {
					String name1 = completeList.get(j);
					for (int m = 0; m < name_bindings.size(); ++m) {
						String name2 = name_bindings.get(m);
						if (name1.equals(name2)) {
							Mapping map = new Mapping();
							map.pos1 = j;
							map.pos2 = m;
							list.add(map);
							if (log.isDebugEnabled()) {
								log.debug("Name = " + name1 + ", map.pos1 = "
										+ map.pos1 + ", map.pos2 = " + map.pos2);
							}
						}
					}
				}
				positionsToJoin.add(list.toArray(new Mapping[list.size()]));

				// The remaining positions go into the positionsToCopy
				List<Integer> positions = new ArrayList<Integer>();
				for (int j = 0; j < name_bindings.size(); ++j) {
					boolean ok = true;
					for (Mapping map : list) {
						if (map.pos2 == j) {
							ok = false;
							break;
						}
					}
					if (ok) {
						positions.add(j);
					}
				}
				int[] pos_not_shared = new int[positions.size()];
				for (int x = 0; x < positions.size(); ++x)
					pos_not_shared[x] = positions.get(x);
				positionsToCopy.add(pos_not_shared);

				// Calculate a map for the join later. Filter the constants
				long[] t = triples.get(i);
				if (log.isDebugEnabled()) {
					log.debug("triples[" + i + "] size = " + t.length);
				}

				// List<long[]> pattern_triples = new ArrayList<long[]>();
				long[] var_triples = new long[t.length / 3 * pos_vars.length];
				int c = 0;
				for (int r = 0; r < t.length; r += 3) {
					// long[] tuple = new long[pos_vars.length];
					for (int w = 0; w < pos_vars.length; ++w) {
						var_triples[c++] = t[r + pos_vars[w]];
					}
					// pattern_triples.add(tuple);
				}
				TupleSet tmpSet = new TupleSet(name_bindings, /* pattern_triples */
				var_triples);

				int[] inputBinding = new int[list.size()];
				for (int x = 0; x < inputBinding.length; ++x) {
					inputBinding[x] = list.get(x).pos2;
				}

				TupleMap map = tmpSet.getBindingsFromBindings(inputBinding,
						null, true);
				mappings.add(map);

				// Update the complete mappings
				for (int y = 0; y < pos_not_shared.length; ++y) {
					completeList.add(name_bindings.get(pos_not_shared[y]));
				}
			}
		}

		/*****
		 * CODE TO HANDLE CASES WHERE THE HEAD IS THE SAME AS THE PRECOMP
		 * PATTERNS
		 *****/
		Set<MultiValue> unique_keys = new HashSet<MultiValue>();
		boolean filter = false;
		int[] dest_pos = null;
		int[] recursive_pos_vars = null;
		if (destination != null) {
			filter = true;

			// Load in unique_keys all the variables
			long[] t = triples.get(pos_pattern);
			recursive_pos_vars = otherPatterns.get(pos_pattern)
					.getPositionVars();
			// TreeSet<MultiValue> uk = new TreeSet<MultiValue>();
			for (int i = 0; i < t.length; i += 3) {
				MultiValue key = new MultiValue(
						new long[recursive_pos_vars.length]);
				for (int j = 0; j < recursive_pos_vars.length; ++j) {
					key.values[j] = t[i + recursive_pos_vars[j]];
				}
				unique_keys.add(key);
			}

			// Calculate the destinations
			dest_pos = new int[recursive_pos_vars.length];
			for (int count = 0; count < recursive_pos_vars.length; ++count) {
				int j = 0;
				for (String name : completeList) {
					if (destination[count].equals(name)) {
						dest_pos[count] = j;
						break;
					}
					j++;
				}
			}

			// Use a compressed version to save memory. Pity that it's O(nlogn).
			// unique_keys = new ComprMultivalueSet(uk,
			// recursive_pos_vars.length);
		}
		/***** END *****/

		List<long[]> output = new ArrayList<long[]>();
		final int[] pos_vars = otherPatterns.get(0).getPositionVars();
		final long[] first_triples = triples.get(0);
		if (mappings.size() == 0) {
			for (int x = 0; x < first_triples.length; x += 3) {
				long[] triple = new long[pos_vars.length];
				for (int i = 0; i < pos_vars.length; ++i) {
					triple[i] = first_triples[x + pos_vars[i]];
				}
				output.add(triple);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("first_triples.length = " + first_triples.length);
			}
			if (first_triples.length > 1000) { // More than 3000 triples to
				// join

				// Split the execution among threads
				int chunk = first_triples.length
						/ QueryPIE.PARALLEL_JOIN_THREADS;
				chunk -= chunk % 3;
				Flag flag = new Flag();
				flag.flag = QueryPIE.PARALLEL_JOIN_THREADS;
				flag.output = output;

				int start = 0;
				long startTime = System.currentTimeMillis();
				int chunkId = 0;
				while (start < first_triples.length) {
					int endChunk = start + chunk;
					if (chunkId++ == QueryPIE.PARALLEL_JOIN_THREADS - 1) {
						endChunk = first_triples.length;
						chunk = endChunk - start;
					}
					RunnableJoin join = new RunnableJoin(flag, first_triples,
							start, endChunk, pos_vars, mappings,
							positionsToJoin, positionsToCopy, filter, dest_pos,
							unique_keys);
					ThreadPool.createNew(join, "Join in parallel " + chunkId);
					start += chunk;
				}

				synchronized (flag) {
					while (flag.flag != 0) {
						flag.wait();
					}
				}
				log.debug("Finished computation: "
						+ (System.currentTimeMillis() - startTime));

			} else {
				perform_join_range_triples(first_triples, 0,
						first_triples.length, pos_vars, mappings,
						positionsToJoin, positionsToCopy, filter, dest_pos,
						unique_keys, output);
			}
		}

		return new TupleSet(completeList, output);
	}

	private static class Flag {
		int flag;
		List<long[]> output;
	}

	private static class RunnableJoin implements Runnable {
		Flag flag;
		long[] first_triples;
		int start;
		int end;
		int[] pos_vars;
		List<TupleMap> mappings;
		List<Mapping[]> positionsToJoin;
		List<int[]> positionsToCopy;
		boolean filter;
		int[] dest_pos;
		Set<MultiValue> unique_keys;

		public RunnableJoin(Flag flag, long[] first_triples, int start,
				int end, int[] pos_vars, List<TupleMap> mappings,
				List<Mapping[]> positionsToJoin, List<int[]> positionsToCopy,
				boolean filter, int[] dest_pos, Set<MultiValue> unique_keys) {
			this.flag = flag;
			this.first_triples = first_triples;
			this.start = start;
			this.end = end;
			this.pos_vars = pos_vars;
			this.mappings = mappings;
			this.positionsToJoin = positionsToJoin;
			this.positionsToCopy = positionsToCopy;
			this.filter = filter;
			this.dest_pos = dest_pos;
			this.unique_keys = unique_keys;
		}

		@Override
		public void run() {
			List<long[]> local_output = new ArrayList<long[]>();
			perform_join_range_triples(first_triples, start, end, pos_vars,
					mappings, positionsToJoin, positionsToCopy, filter,
					dest_pos, unique_keys, local_output);

			synchronized (flag) {
				flag.output.addAll(local_output);
				flag.flag--;
				flag.notify();
			}

			// Cleanup all the datastructures
			flag = null;
			first_triples = null;
			mappings = null;
			unique_keys = null;
		}
	}

	private static void perform_join_range_triples(long[] first_triples,
			int start, int end, int[] pos_vars, List<TupleMap> mappings,
			List<Mapping[]> positionsToJoin, List<int[]> positionsToCopy,
			boolean filter, int[] dest_pos, Set<MultiValue> unique_keys,
			List<long[]> output) {

		long[] triple = new long[pos_vars.length];
		MultiValue key = new MultiValue(new long[2]);
		int[] counters = new int[mappings.size()];
		CollectionTuples[] intermediateResults = new CollectionTuples[mappings
		                                                              .size()];
		long[][] current_tuples = new long[mappings.size() + 1][];
		current_tuples[0] = triple;
		MultiValue uk = null;
		if (filter)
			uk = new MultiValue(new long[dest_pos.length]);

		for (int x = start; x < end; x += 3) {

			for (int i = 0; i < pos_vars.length; ++i) {
				triple[i] = first_triples[x + pos_vars[i]];
			}

			int next_key = 0;
			do {
				CollectionTuples current_collection = intermediateResults[next_key];
				if (current_collection == null) {
					// Create a key
					Mapping[] joinPos = positionsToJoin.get(next_key);

					if (joinPos.length == 1) {
						current_collection = mappings.get(next_key).getLong(
								current_tuples[next_key][joinPos[0].pos1]);
					} else {
						for (int i = 0; i < joinPos.length; ++i) {
							key.values[i] = current_tuples[next_key][joinPos[i].pos1];
						}
						current_collection = mappings.get(next_key).get(key);
					}
					intermediateResults[next_key] = current_collection;

					if (current_collection == null
							|| current_collection.getNTuples() == 0) {
						intermediateResults[next_key] = null;
						next_key--;
						continue;
					}
					counters[next_key] = current_collection.getStart();
				} else {
					counters[next_key] += current_collection.getSizeTuple();
				}

				if (counters[next_key] < current_collection.getEnd()) {
					// Generate a tuple with the first key
					if (next_key == counters.length - 1) {
						long[] new_tuple = concatenateTuples(
								current_tuples[next_key], 0,
								current_tuples[next_key].length,
								current_collection.getRawValues(),
								counters[next_key],
								positionsToCopy.get(next_key));

						if (filter) {
							for (int i = 0; i < dest_pos.length; ++i) {
								uk.values[i] = new_tuple[dest_pos[i]];
							}

							if (!unique_keys.contains(uk)) {
								output.add(new_tuple);
							}
						} else {
							output.add(new_tuple);
						}
					} else {
						if (current_tuples[next_key + 1] != null) {
							concatenateTuples(current_tuples[next_key + 1],
									current_tuples[next_key], 0,
									current_tuples[next_key].length,
									current_collection.getRawValues(),
									counters[next_key],
									positionsToCopy.get(next_key));
						} else {
							current_tuples[next_key + 1] = concatenateTuples(
									current_tuples[next_key], 0,
									current_tuples[next_key].length,
									current_collection.getRawValues(),
									counters[next_key],
									positionsToCopy.get(next_key));
						}

						next_key++;
					}
				} else {
					intermediateResults[next_key] = null;
					next_key--;
				}

			} while (next_key >= 0);

		}
	}

	protected void join_tuple(MultiValue key, long[] tuple, int position,
			List<Mapping[]> positionsToJoin, List<int[]> positionsToCopy,
			List<TupleMap> mappings) {
		if (position < positionsToJoin.size()) {

			Mapping[] join_points = positionsToJoin.get(position);
			CollectionTuples col = null;
			if (join_points.length != 1) {
				for (int i = 0; i < join_points.length; ++i) {
					key.values[i] = tuple[join_points[i].pos1];
				}
				col = mappings.get(position).get(key);
			} else {
				col = mappings.get(position)
						.getLong(tuple[join_points[0].pos1]);
			}

			if (col != null) {
				for (int i = col.getStart(); i < col.getEnd(); i += col
						.getSizeTuple()) {
					// Create a new tuple
					long[] new_tuple = concatenateTuples(tuple, 0,
							tuple.length, col.getRawValues(), i,
							positionsToCopy.get(position));

					// Call recursively the method
					if (position + 1 == positionsToJoin.size()) {
						// System.out.println("Triple: " +
						// Arrays.toString(tuple));
					} else {
						join_tuple(key, new_tuple, position + 1,
								positionsToJoin, positionsToCopy, mappings);
					}
				}
			}
		}
	}

	public TupleSet join(Pattern pattern, long[] triples, String[] origin,
			String[] destination) throws Exception {

		if (nameBindings.size() == 0) {
			int[] pos_to_copy = new int[3];
			int n_pos_to_copy = 0;

			int i = 0;

			for (RDFTerm t : pattern.p) {
				if (t.getName() != null) {
					nameBindings.add(t.getName());
					sizeTuple++;
					pos_to_copy[n_pos_to_copy++] = i;
				}
				i++;
			}

			nTuples = triples.length / 3;
			currentSize = nTuples * sizeTuple;
			bindings = new long[currentSize];
			int copy_pos = 0;
			for (int m = 0; m < triples.length; m += 3) {
				for (int j = 0; j < n_pos_to_copy; ++j) {
					bindings[copy_pos + j] = triples[m + pos_to_copy[j]];
				}
				copy_pos += sizeTuple;
			}
			threshold = -1;

			return this;

		} else {

			// Determine the common join points between the two sets
			Mapping[] shared_vars = calculateSharedVariables(pattern);
			int[] pos_not_shared_vars = calculatePositionNotSharedVariables(pattern);
			List<String> newBindings = concatenateBindings(pattern);

			// List<Integer> pos_exclude = null;
			// if (exclude_fields != null) {
			// pos_exclude = new ArrayList<Integer>();
			// for(String field : exclude_fields) {
			// int count = 0;
			// for(String binding : newBindings) {
			// if (binding.equals(field)) {
			// pos_exclude.add(count);
			// }
			// count++;
			// }
			// }
			// }

			// This data structure is needed to filter eventual duplicates
			int[] pos_dest = null;
			Set<MultiValue> unique_keys = new HashSet<MultiValue>();
			MultiValue uk = null;
			if (origin != null && destination != null) {
				// Calculate destination keys pos
				pos_dest = calculatePositionMappings(newBindings, destination);
				int[] pos_orig = calculatePositionMappings(nameBindings, origin);

				if (pos_dest != null) {
					// Calculate origin keys pos
					if (pos_orig != null) {
						for (int x = 0; x < currentSize; x += sizeTuple) {
							MultiValue v = new MultiValue(
									new long[pos_orig.length]);
							for (int i = 0; i < pos_orig.length; ++i) {
								v.values[i] = bindings[x + pos_orig[i]];
							}
							unique_keys.add(v);
						}
					} else {
						List<String> vars = new ArrayList<String>();
						for (RDFTerm t : pattern.p) {
							vars.add(t.getName());
						}
						pos_orig = calculatePositionMappings(vars, origin);
						if (pos_orig != null) {
							for (int x = 0; x < triples.length; x += 3) {
								MultiValue v = new MultiValue(
										new long[pos_orig.length]);
								for (int i = 0; i < pos_orig.length; ++i) {
									v.values[i] = triples[x + pos_orig[i]];
								}
								unique_keys.add(v);
							}
						}
					}
				}

				if (pos_orig != null && pos_dest != null) {
					uk = new MultiValue(new long[pos_dest.length]);
				} else {
					pos_dest = null;
				}
			}

			int[] input = new int[shared_vars.length];
			for (int j = 0; j < input.length; ++j) {
				input[j] = shared_vars[j].pos1;
			}
			TupleMap side1 = getBindingsFromBindings(input, null, true);

			if (side1 != null) {
				// Do the join between the first and the second side
				List<long[]> results = new ArrayList<long[]>();
				MultiValue v = new MultiValue(new long[shared_vars.length]);

				// Set<MultiValue> duplicates = new HashSet<MultiValue>();

				for (int x = 0; x < triples.length; x += 3) {

					// if (x % 100 == 0)
					// log.warn("Join triple " + x + " size results="
					// + results.size() + " duplicates="
					// + duplicates.size());

					for (int j = 0; j < shared_vars.length; ++j)
						v.values[j] = triples[x + shared_vars[j].pos2];
					CollectionTuples vals = side1.get(v);
					if (vals != null) {
						int sizeTuple = vals.getSizeTuple();
						long[] rawValues = vals.getRawValues();
						for (int y = vals.getStart(); y < vals.getEnd(); y += sizeTuple) {
							long[] concat_tuple = concatenateTuples(rawValues,
									y, sizeTuple, triples, x,
									pos_not_shared_vars);

							// long[] copy = Arrays.copyOf(concat_tuple,
							// concat_tuple.length);
							// MultiValue dupl = new MultiValue(copy);
							// if (!duplicates.contains(dupl)) {

							if (pos_dest != null) {
								for (int i = 0; i < pos_dest.length; ++i) {
									uk.values[i] = concat_tuple[pos_dest[i]];
								}
								if (!unique_keys.contains(uk)) {
									results.add(concat_tuple);
									// duplicates.add(dupl);
								}
							} else {
								results.add(concat_tuple);
								// duplicates.add(dupl);
							}
							// }
						}
					}
				}

				return new TupleSet(newBindings, results);
			}

			return null;
		}
	}

	public int[] calculatePositionMappings(List<String> bindings,
			String[] mappings) {
		int[] positions = new int[mappings.length];

		for (int i = 0; i < mappings.length; i++) {
			String mapping = mappings[i];
			int pos = 0;
			boolean found = false;
			for (String binding : bindings) {
				if (binding != null && binding.equals(mapping)) {
					positions[i] = pos;
					found = true;
					break;
				}
				pos++;
			}

			if (!found)
				return null;

		}

		return positions;
	}

	public int[] calculatePositionNotSharedVariables(Pattern pattern) {
		List<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < 3; ++i) {
			RDFTerm t = pattern.p[i];
			if (t.getName() != null) {
				boolean found = false;
				for (String s : nameBindings) {
					if (t.getName().equals(s)) {
						found = true;
						break;
					}
				}
				if (!found) {
					list.add(i);
				}
			}
		}
		int[] pos = new int[list.size()];
		int i = 0;
		for (int p : list) {
			pos[i++] = p;
		}
		return pos;
	}

	public TupleSet filter(int list_pos, Collection<Long> lists) {
		String key = "filter(" + list_pos + ")";
		TupleSet set = (TupleSet) cache.get(key);

		if (set == null) {
			set = new TupleSet(nameBindings);
			long[] tuple = new long[sizeTuple];
			for (int i = 0; i < nTuples * sizeTuple; i += sizeTuple) {
				if (lists.contains(bindings[i + list_pos])) {
					System.arraycopy(bindings, i, tuple, 0, sizeTuple);
					set.addTuple(tuple);
				}
			}
			cache.put(key, set);
		}
		return set;
	}
}
