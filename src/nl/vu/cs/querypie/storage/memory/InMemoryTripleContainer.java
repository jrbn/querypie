package nl.vu.cs.querypie.storage.memory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryTripleContainer implements Serializable {

	private static final long serialVersionUID = -510514286176150005L;

	static final Logger log = LoggerFactory
			.getLogger(InMemoryTripleContainer.class);

	private Map<String, List<List<Collection<Long>>>> queries = new HashMap<String, List<List<Collection<Long>>>>();
	private Collection<Triple> set = new HashSet<Triple>();
	private Triple triple = new Triple();

	private boolean compress = false;
	private Map<Long, Collection<Triple>> s;
	private Map<Long, Collection<Triple>> p;
	private Map<Long, Collection<Triple>> o;

	public InMemoryTripleContainer(boolean compress) {
		this.compress = compress;
		if (compress) {
			s = new CompressedInMemoryIndex();
			p = new CompressedInMemoryIndex();
			o = new CompressedInMemoryIndex();
		} else {
			s = new HashMap<Long, Collection<Triple>>();
			p = new HashMap<Long, Collection<Triple>>();
			o = new HashMap<Long, Collection<Triple>>();
		}
	}

	public InMemoryTripleContainer() {
		this(false);
	}

	public void clear() {
		queries.clear();
		set.clear();
		if (s != null)
			s.clear();
		if (p != null)
			p.clear();
		if (o != null)
			o.clear();
	}

	@Override
	public String toString() {
		return "InMemoryTripleContainer, size = " + set.size();
	}

	public boolean containsTriple(Triple triple) {
		return set.contains(triple);
	}

	public boolean addTriple(Triple t, InMemoryTripleContainer input) {

		if (input != null) {
			synchronized (input) {
				if (input.containsTriple(t)) {
					return false;
				}
			}
		}

		if (!set.add(t)) {
			return false;
		}
		return true;
	}

	public boolean addTriple(RDFTerm a, RDFTerm b, RDFTerm c,
			InMemoryTripleContainer input) {
		this.triple.subject = a.getValue();
		this.triple.predicate = b.getValue();
		this.triple.object = c.getValue();

		if (input != null) {
			synchronized (input) {
				if (input.containsTriple(this.triple)) {
					return false;
				}
			}
		}

		if (!set.add(this.triple)) {
			return false;
		}
		this.triple = new Triple();
		return true;
	}

	public boolean addTriple(RDFTerm a, RDFTerm b, RDFTerm c,
			InMemoryTripleContainer input, InMemoryTripleContainer input2) {
		this.triple.subject = a.getValue();
		this.triple.predicate = b.getValue();
		this.triple.object = c.getValue();

		if (input != null) {
			synchronized (input) {
				if (input.containsTriple(this.triple)) {
					return false;
				}
			}
		}
		if (input2 != null) {
			synchronized (input2) {
				if (input2.containsTriple(this.triple)) {
					return false;
				}
			}
		}

		if (!set.add(this.triple)) {
			return false;
		}
		this.triple = new Triple();
		return true;
	}

	public int size() {
		return set.size();
	}

	public synchronized void addAll(InMemoryTripleContainer set) {
		this.set.addAll(set.set);

		// Add queries
		for (Map.Entry<String, List<List<Collection<Long>>>> entry : set.queries
				.entrySet()) {
			List<List<Collection<Long>>> existingList = this.queries.get(entry
					.getKey());
			if (existingList == null) {
				this.queries.put(entry.getKey(), entry.getValue());
			} else {
				for (List<Collection<Long>> col : entry.getValue()) {
					// Check whether the list "col" is not already present
					boolean match = false;
					for (List<Collection<Long>> list : existingList) {
						match = true;
						for (int i = 0; i < list.size(); ++i) {
							if (col.get(i) != list.get(i)
									&& !col.get(i).equals(list.get(i))) {
								match = false;
								break;
							}
						}
						if (match)
							break;
					}
					if (!match) {
						existingList.add(col);
					}
				}
			}
		}
	}

	public boolean containsTriple(RDFTerm[] t) {
		triple.subject = t[0].getValue();
		triple.predicate = t[1].getValue();
		triple.object = t[2].getValue();
		return set.contains(triple);
	}

	public void index() {

		if (!compress) {
			s = new HashMap<Long, Collection<Triple>>();
			p = new HashMap<Long, Collection<Triple>>();
			o = new HashMap<Long, Collection<Triple>>();

			Triple[] list = set.toArray(new Triple[set.size()]);
			Arrays.sort(list);

			for (Triple triple : list) {
				Collection<Triple> sTriples = s.get(triple.subject);
				if (sTriples == null) {
					sTriples = new ArrayList<Triple>();
					s.put(triple.subject, sTriples);
				}
				sTriples.add(triple);

				Collection<Triple> pTriples = p.get(triple.predicate);
				if (pTriples == null) {
					pTriples = new ArrayList<Triple>();
					p.put(triple.predicate, pTriples);
				}
				pTriples.add(triple);

				Collection<Triple> oTriples = o.get(triple.object);
				if (oTriples == null) {
					oTriples = new ArrayList<Triple>();
					o.put(triple.object, oTriples);
				}
				oTriples.add(triple);
			}
		} else {

			Triple[] new_triples = set.toArray(new Triple[set.size()]);
			Triple[] old_triples = ((CompressedInMemoryIndex) s).triples;
			Triple[] list;
			if (old_triples != null) {
				list = new Triple[old_triples.length + new_triples.length];
				System.arraycopy(old_triples, 0, list, 0, old_triples.length);
				System.arraycopy(new_triples, 0, list, old_triples.length,
						new_triples.length);
			} else {
				list = new_triples;
			}

			Arrays.sort(list);
			// Index the triples in set by s, p, o
			Map<Long, Collection<Triple>> s = new TreeMap<Long, Collection<Triple>>();
			Map<Long, Collection<Triple>> p = new TreeMap<Long, Collection<Triple>>();
			Map<Long, Collection<Triple>> o = new TreeMap<Long, Collection<Triple>>();

			for (Triple triple : list) {
				Collection<Triple> sTriples = s.get(triple.subject);
				if (sTriples == null) {
					sTriples = new ArrayList<Triple>();
					s.put(triple.subject, sTriples);
				}
				sTriples.add(triple);

				Collection<Triple> pTriples = p.get(triple.predicate);
				if (pTriples == null) {
					pTriples = new ArrayList<Triple>();
					p.put(triple.predicate, pTriples);
				}
				pTriples.add(triple);

				Collection<Triple> oTriples = o.get(triple.object);
				if (oTriples == null) {
					oTriples = new ArrayList<Triple>();
					o.put(triple.object, oTriples);
				}
				oTriples.add(triple);
			}

			this.s = compressIndex(s);
			this.p = compressIndex(p);
			this.o = compressIndex(o);
			set.clear();
		}
	}

	private CompressedInMemoryIndex compressIndex(
			Map<Long, Collection<Triple>> input) {
		// Compress the indices
		ArrayList<Triple> list1 = new ArrayList<Triple>();
		long[] keys = new long[input.size()];
		int[] starts = new int[input.size()];
		int[] length = new int[input.size()];
		int counter = 0;
		for (Map.Entry<Long, Collection<Triple>> col : input.entrySet()) {
			keys[counter] = col.getKey();
			starts[counter] = list1.size();
			length[counter] = col.getValue().size();
			// Copy the triples
			for (Triple t : col.getValue()) {
				list1.add(t);
			}
			counter++;
		}
		return new CompressedInMemoryIndex(keys, starts, length,
				list1.toArray(new Triple[list1.size()]));
	}

	public Map<Long, Collection<Triple>> getTriplesIndexedBySubject() {
		return s;
	}

	public Map<Long, Collection<Triple>> getTriplesIndexedByPredicate() {
		return p;
	}

	public Map<Long, Collection<Triple>> getTriplesIndexedByObject() {
		return o;
	}

	public Collection<Triple> getTripleSet() {
		return set;
	}

	public void addQuery(long v1, long v2, long v3, ActionContext context,
			InMemoryTripleContainer input) {
		String key = v1 + " " + v2 + " " + v3;
		if (!queries.containsKey(key)
				&& (input == null || (input != null && !input.queries
						.containsKey(key)))) {
			queries.put(key, null);

			// Add a new key with the cardinality of the set
			List<Collection<Long>> sets = new ArrayList<Collection<Long>>();
			if (v1 <= RDFTerm.THRESHOLD_VARIABLE) {
				key = "* ";
				sets.add(Schema.getInstance().getSubset(v1, context));
			} else {
				key = v1 + " ";
			}

			if (v2 <= RDFTerm.THRESHOLD_VARIABLE) {
				key += "* ";
				sets.add(Schema.getInstance().getSubset(v3, context));
			} else {
				key += v2 + " ";
			}

			if (v3 <= RDFTerm.THRESHOLD_VARIABLE) {
				key += "*";
				sets.add(Schema.getInstance().getSubset(v3, context));
			} else {
				key += "" + v3;
			}

			if (sets.size() > 0) {
				List<List<Collection<Long>>> lists = queries.get(key);
				if (lists == null) {
					lists = new ArrayList<List<Collection<Long>>>();
					queries.put(key, lists);
				}
				lists.add(sets);
			}

		}
	}

	public boolean containsQuery(long v1, long v2, long v3,
			ActionContext context) throws Exception {
		String key = v1 + " " + v2 + " " + v3;
		if (!queries.containsKey(key)) {
			// Check whether there are some set of triples. If there are,
			// construct a new key using the cardinality.
			List<Collection<Long>> sets = new ArrayList<Collection<Long>>();
			if (v1 <= RDFTerm.THRESHOLD_VARIABLE) {
				Collection<Long> set = Schema.getInstance().getSubset(v1,
						context);
				key = "* ";
				sets.add(set);
			} else {
				key = v1 + " ";
			}

			if (v2 <= RDFTerm.THRESHOLD_VARIABLE) {
				Collection<Long> set = Schema.getInstance().getSubset(v2,
						context);
				key += "* ";
				sets.add(set);
			} else {
				key += v2 + " ";
			}

			if (v3 <= RDFTerm.THRESHOLD_VARIABLE) {
				Collection<Long> set = Schema.getInstance().getSubset(v3,
						context);
				key += "*";
				sets.add(set);
			} else {
				key += "" + v3;
			}

			if (sets.size() > 0 && queries.containsKey(key)) {
				// There is a pattern with the sets with the same cardinality.
				// Very probable it is the same query.
				List<List<Collection<Long>>> lists = queries.get(key);

				if (sets.size() == 1) {
					@SuppressWarnings("unchecked")
					Collection<Long>[] col = new Collection[lists.size()];
					for (int i = 0; i < col.length; ++i) {
						col[i] = lists.get(i).get(0);
					}
					for (long value : sets.get(0)) {
						boolean found = false;
						for (Collection<Long> c : col) {
							if (c.contains(value)) {
								found = true;
								break;
							}
						}
						if (!found) {
							return false;
						}
					}
					return true;
				} else if (sets.size() == 2) {
					// Here I cannot check whether the values appear on more
					// sets.
					Collection<Long> s1 = sets.get(0);
					Collection<Long> s2 = sets.get(1);

					for (int i = 0; i < lists.size(); ++i) {
						Collection<Long> as1 = lists.get(i).get(0);
						Collection<Long> as2 = lists.get(i).get(1);
						if (s1.size() == as1.size() && s2.size() == as2.size()) {
							Iterator<Long> itr1 = s1.iterator();
							Iterator<Long> aitr1 = as1.iterator();
							while (itr1.hasNext()) {
								if (itr1.next().longValue() != aitr1.next()
										.longValue())
									return false;
							}

							Iterator<Long> itr2 = s2.iterator();
							Iterator<Long> aitr2 = as2.iterator();
							while (itr2.hasNext()) {
								if (itr2.next().longValue() != aitr2.next()
										.longValue())
									return false;
							}

							return true;

						}
					}
					return false;
				} else {
					throw new Exception("Not supported");
				}
			}

			return false;
		}
		return true;
	}

	public boolean isToCopy() {
		return size() > 0 || queries.size() > 0;
	}

	public void removeAll(InMemoryTripleContainer explicitTriples) {
		set.removeAll(explicitTriples.set);
	}
}
